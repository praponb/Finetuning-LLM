from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc(), "id")
w_recent = Window.orderBy(F.col("shipping_fee").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("line_total").desc_nulls_last(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee_dense_rank").desc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.year(F.col("p.order_date")) >= 2023)
    .select("src.id", "src.order_id", "src.line_total", F.col("p.shipping_fee"))
)

with_windows = (
    joined
    .withColumn("shipping_fee_bucket", F.ntile(4).over(w))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("line_total_avg_6", F.avg("line_total").over(w3))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "line_total",
        "shipping_fee",
        "shipping_fee_bucket",
        "shipping_fee_dense_rank",
        "line_total_avg_6",
        "rn",
    )
)
