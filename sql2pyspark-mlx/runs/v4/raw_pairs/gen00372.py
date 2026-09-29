from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

with_windows = (
    df_order_items
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("unit_price_bucket", F.ntile(3).over(w2))
    .withColumn("quantity_bucket", F.ntile(5).over(w3))
)

result = (
    with_windows
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
    )
    .orderBy(F.col("order_id").desc_nulls_last())
)
