from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price_rounded", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price_rounded", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price_rounded").desc(), "id")
w_running = Window.orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("stock_qty").desc(), "id")

joined = (
    df_order_items
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.launch_date").between("2024-07-01", "2025-08-01"))
    .select("a.id", "a.order_id", "a.product_id", "a.unit_price", F.col("d.stock_qty"))
)

valid = (
    joined
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)

enriched = (
    valid
    .select(
        "id",
        "product_id",
        "unit_price",
        "stock_qty",
        "unit_price_rounded",
        F.ntile(5).over(w).alias("unit_price_rounded_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "stock_qty", "unit_price_rounded")
)

metrics = (
    ranked
    .withColumn("running_sum_unit_price_rounded", F.sum("unit_price_rounded").over(w_running))
    .withColumn("running_max_unit_price_rounded", F.max("unit_price_rounded").over(w_running))
    .withColumn("running_avg_unit_price_rounded", F.avg("unit_price_rounded").over(w_running))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "stock_qty",
        "running_sum_unit_price_rounded",
        "running_avg_unit_price_rounded",
    )
    .orderBy(F.col("running_sum_unit_price_rounded").asc_nulls_last())
)
