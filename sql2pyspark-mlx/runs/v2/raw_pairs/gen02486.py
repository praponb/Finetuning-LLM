from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNull() & ((F.col("account_type") == "LOAN") | (F.col("status") != "CLOSED")))
    .withColumn("clean_account_type", F.trim("account_type"))
)

account_type_summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    account_type_summary
    .filter(F.col("savings_balance") > 37)
    .select("account_type", "status_values")
    .orderBy("account_type")
)
