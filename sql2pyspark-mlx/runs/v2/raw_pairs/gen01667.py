from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-06-15")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") >= 40000)
    .orderBy(F.col("balance_count").desc())
)
