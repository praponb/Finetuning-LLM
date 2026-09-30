from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type") == "LOAN"))
    .groupBy("status")
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") >= 69)
)
