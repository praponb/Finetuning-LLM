from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner"]))
    .groupBy("status", "store_id", "customer_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.countDistinct("discount").alias("unique_statuss"),
    )
)
