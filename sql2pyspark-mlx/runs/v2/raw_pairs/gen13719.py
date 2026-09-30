from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([1, 18, 25]) & F.col("customer_id").isin([4, 25, 27, 33]) & (F.col("customer_id") == 20))
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .orderBy("status")
    .limit(10)
)
