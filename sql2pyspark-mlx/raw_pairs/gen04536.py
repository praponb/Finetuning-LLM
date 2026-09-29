from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("account_type") == "LOAN")

accounts_stats = (
    filtered_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
)

result = (
    accounts_stats
    .filter(F.col("pct_checking") >= 59)
    .orderBy("status")
)
