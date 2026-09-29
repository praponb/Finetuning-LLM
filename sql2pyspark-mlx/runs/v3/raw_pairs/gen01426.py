from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .withColumn("account_type_prefix", F.substring("account_type", 1, 3))
)

status_summary = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = status_summary.select("status", "account_type_list", "latest_opened_date")
