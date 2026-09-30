from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("max_balance") < 50000)
    .select("account_type", "customer_id", "max_balance", "status_list")
)
