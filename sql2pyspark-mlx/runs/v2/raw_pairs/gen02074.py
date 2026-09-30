from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc(), "unique_accounts")
)
