from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(~((F.col("account_type") == "SAVINGS") & (F.col("balance") <= 69000) & (F.col("balance") <= 48000)))
)

accounts_stats = (
    base
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = accounts_stats.filter(F.col("pct_loan") > 33)
