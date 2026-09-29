from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isNull() & (F.col("status") == "OPEN"))
    .withColumn("account_type_length", F.length("account_type"))
)

account_type_summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("account_type_length")).otherwise(0)).alias("open_account_type_length"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
    )
)

result = (
    account_type_summary
    .filter(F.col("open_count") >= 38)
    .select("account_type", "open_account_type_length", "open_count")
    .orderBy("account_type")
)
