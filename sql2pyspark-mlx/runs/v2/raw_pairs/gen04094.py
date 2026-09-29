from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
