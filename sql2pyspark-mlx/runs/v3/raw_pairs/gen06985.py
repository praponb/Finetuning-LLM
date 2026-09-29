from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("*").alias("account_count"),
    )
    .orderBy("account_count")
)
