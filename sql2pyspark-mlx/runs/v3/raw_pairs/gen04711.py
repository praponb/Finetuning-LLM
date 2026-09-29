from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-06-01")
    .groupBy("account_type", "customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") < 26000)
)
