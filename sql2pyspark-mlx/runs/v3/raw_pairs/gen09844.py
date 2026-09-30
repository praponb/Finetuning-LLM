from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("opened_date").between("2023-12-20", "2024-03-01") & F.col("customer_id").isNull())
    .withColumn("account_type_status", F.concat("account_type", F.lit("/"), "status"))
)

status_summary = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
        F.count("balance").alias("balance_count"),
    )
)

result = (
    status_summary
    .filter(F.col("pct_frozen") < 72)
    .select("status", "balance_count")
)
