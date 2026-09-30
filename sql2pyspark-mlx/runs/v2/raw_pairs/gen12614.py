from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") > "2024-11-01"))
)

summary = (
    base
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)

result = summary.select("status", "account_type_list")
