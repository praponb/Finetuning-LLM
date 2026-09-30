from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "LOAN") & (F.col("opened_date") < "2024-07-01"))
    .groupBy("status", "account_type")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
