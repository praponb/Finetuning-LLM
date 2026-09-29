from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(23000, 67000) & F.col("customer_id").isNull())
    .groupBy("status")
    .agg(F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"))
)
