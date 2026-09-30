from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "status")
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
