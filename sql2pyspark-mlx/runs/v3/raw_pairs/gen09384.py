from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.year("opened_date") >= 2023) & F.col("account_type").isin(["SAVINGS", "LOAN"]))
)

accounts_stats = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    accounts_stats
    .filter(F.col("pct_frozen") >= 24)
    .orderBy("account_type")
)
