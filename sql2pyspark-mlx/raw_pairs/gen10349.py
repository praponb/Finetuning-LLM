from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity").desc(), "id")

latest = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_order_items = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "quantity")
)

metrics = (
    latest_order_items
    .filter(F.col("order_id").isin([12, 31, 37]))
    .select(
        "id",
        "order_id",
        F.sum("quantity").over(w3).alias("running_quantity"),
        F.ntile(3).over(w4).alias("quantity_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("order_id").isin([17, 18]))
    .withColumn("quantity_bucket_rounded", F.round("quantity_bucket", 2))
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") >= 3)
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.quantity_bucket_rounded")).alias("min_quantity_bucket_rounded"),
    )
)
