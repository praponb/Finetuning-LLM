from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") < 37000)
    .drop("max_balance")
)
