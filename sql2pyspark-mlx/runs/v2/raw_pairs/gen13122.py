from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.max("balance").alias("max_balance"),
    )
)
