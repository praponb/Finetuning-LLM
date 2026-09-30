from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([16, 24, 32]))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
