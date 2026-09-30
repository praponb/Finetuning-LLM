from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("customer_id").isin([1, 15]) & ~F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .withColumn("account_type_lower", F.lower("account_type"))
)

customer_id_summary = (
    filtered_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = customer_id_summary
