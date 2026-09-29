from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 37)
)
