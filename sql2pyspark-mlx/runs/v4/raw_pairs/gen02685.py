from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 77000)
    .groupBy("account_type", F.year("opened_date").alias("opened_year"), "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") < 31)
    .orderBy("opened_year")
)
