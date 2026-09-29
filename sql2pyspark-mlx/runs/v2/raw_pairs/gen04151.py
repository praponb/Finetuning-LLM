from pyspark.sql import functions as F

result = (
    df_accounts
    .cube("account_type", "status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("status", F.col("account_type").desc())
)
