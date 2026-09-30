from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("account_type") != "CHECKING") & (F.year("opened_date") == 2024))
)

accounts_stats = (
    recent_accounts
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = accounts_stats.orderBy("status")
