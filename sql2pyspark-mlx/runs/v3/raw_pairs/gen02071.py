from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.25)
    .groupBy("channel")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy(F.col("channel").desc(), F.col("stddev_amount").desc())
)
