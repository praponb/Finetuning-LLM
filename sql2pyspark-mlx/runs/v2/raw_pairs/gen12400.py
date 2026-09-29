from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy("method", F.col("stddev_amount").desc())
    .limit(1)
)
