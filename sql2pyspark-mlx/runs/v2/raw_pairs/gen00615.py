from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("status") == "FROZEN")

customer_id_summary = (
    valid_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    customer_id_summary
    .select("customer_id", "account_type_list")
    .orderBy("customer_id")
)
