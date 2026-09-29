from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("amount").between(550, 670) & (F.col("amount") < 1240)))
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
