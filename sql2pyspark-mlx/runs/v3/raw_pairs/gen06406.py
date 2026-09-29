from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "mobile"]))
    .groupBy(
        F.when(F.col("channel").isin(["partner", "web"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("discount").alias("total_discount"),
    )
)
