from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
