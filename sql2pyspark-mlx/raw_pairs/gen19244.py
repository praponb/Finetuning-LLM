from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee").alias("order_shipping_fee"))
)

result = (
    with_latest
    .groupBy("region")
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.max("order_shipping_fee").alias("max_order_shipping_fee"),
        F.max(F.when(F.col("city") == "Singapore", F.col("sqft"))).alias("singapore_max_sqft"),
    )
    .orderBy(F.col("max_order_shipping_fee").asc_nulls_last())
)
