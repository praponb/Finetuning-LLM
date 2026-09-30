from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("opened_date").alias("latest_opened_date"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
    .filter(F.col("frozen_balance") > 55)
)
