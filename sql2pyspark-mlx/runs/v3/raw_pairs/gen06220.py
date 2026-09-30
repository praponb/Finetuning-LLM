from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 58)
    .orderBy(F.col("stddev_amount").desc(), "channel")
    .limit(10)
)
