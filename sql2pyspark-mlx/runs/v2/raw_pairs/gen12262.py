from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.18) & F.col("channel").isin(["store", "web", "partner"]))
    .groupBy("channel", "status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 28)
    .orderBy(F.col("channel").desc())
)
