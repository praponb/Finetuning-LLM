from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max("opened_date").alias("latest_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)
