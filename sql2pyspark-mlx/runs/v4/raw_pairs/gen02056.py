from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_scaled", "id")
w2 = Window.orderBy("quantity", "id")
w_running = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity_scaled").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("quantity")
w7 = Window.partitionBy("channel").orderBy("unit_price", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("status").orderBy(F.col("quantity_bucket_2").desc_nulls_last())
w9 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([21, 30, 35]))
    .select("*", F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .withColumn("quantity_bucket", F.ntile(5).over(w2))
    .withColumn("quantity_bucket_2", F.ntile(3).over(w_running))
    .withColumn("quantity_scaled_bucket", F.ntile(3).over(w4))
    .withColumn("quantity_bucket_3", F.ntile(3).over(w5))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.amount"), F.col("b.channel"), F.col("b.status"))
)

result = (
    with_dims
    .withColumn("quantity_rank", F.rank().over(w6))
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w7))
    .withColumn("quantity_bucket_2_cume_dist", F.cume_dist().over(w8))
    .withColumn("amount_percent_rank", F.percent_rank().over(w9))
)
