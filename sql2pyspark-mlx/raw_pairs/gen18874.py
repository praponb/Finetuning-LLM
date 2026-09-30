from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("avg_balance") < 29)
    .select("account_type", "avg_balance", "balance_range")
)
