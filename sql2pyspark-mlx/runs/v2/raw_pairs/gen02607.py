from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]) & (F.col("balance") > 64000))
)

accounts_stats = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    accounts_stats
    .select("account_type", "status_list")
    .orderBy("account_type")
)
