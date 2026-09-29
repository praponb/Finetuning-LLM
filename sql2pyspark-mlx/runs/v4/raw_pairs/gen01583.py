from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w_part = Window.orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

with_windows = (
    df_stores
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("event_seq", F.row_number().over(w_part))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.customer_id").isNotNull()), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"), F.col("c.discount"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("week", "order_order_date").alias("week_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
    )
    .orderBy(F.col("discount_p50").desc())
)
