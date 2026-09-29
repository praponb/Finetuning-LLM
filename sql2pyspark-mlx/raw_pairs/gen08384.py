from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("customer_id").isNull() | (F.col("balance") > 22000))

accounts_stats = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    accounts_stats
    .select("status", "status_values", "last_opened_date")
    .orderBy("status_values", "status")
    .limit(10)
)
