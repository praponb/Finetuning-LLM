from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-12-09")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("unique_accounts")
)
