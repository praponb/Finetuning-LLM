from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "order_order_date"), "order_id").orderBy(F.col("running_count_amount").desc(), "id")
w4 = Window.orderBy("running_max_amount", "id")
w_ordered = Window.orderBy("running_count_amount", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.shipping_fee"), F.col("d.status"), F.col("d.order_date"))
)

with_dims_order_items = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.quantity",
        "a.unit_price",
        "a.status",
        F.col("p.amount"),
        F.col("p.order_date").alias("order_order_date"),
    )
)

with_windows = (
    with_dims_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "status",
        "amount",
        "order_order_date",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
    )
)

enriched = (
    with_windows
    .filter(F.col("running_max_amount") > 490)
    .withColumn("running_count_amount_bucket", F.ntile(3).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("running_count_amount_bucket_2", F.ntile(10).over(w_ordered))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        F.col("running_max_amount").alias("RUNNING_MAX_AMOUNT_VAL"),
        F.col("first_known_unit_price").alias("FIRST_KNOWN_UNIT_PRICE_VAL"),
        "running_sum_amount",
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["RUNNING_MAX_AMOUNT_VAL", "FIRST_KNOWN_UNIT_PRICE_VAL", "running_sum_amount"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
