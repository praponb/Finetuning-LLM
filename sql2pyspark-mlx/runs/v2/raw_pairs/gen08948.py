from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("opened_date") > "2025-06-24")

accounts_stats = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("balance").alias("balance_count"),
    )
)

result = (
    accounts_stats
    .select("account_type", "status_list", "balance_count")
    .orderBy("account_type")
)
