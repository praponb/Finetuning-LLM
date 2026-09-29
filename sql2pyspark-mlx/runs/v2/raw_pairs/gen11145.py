from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-03-01")
    .groupBy("account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .orderBy("avg_balance", "account_type")
    .limit(3)
)
