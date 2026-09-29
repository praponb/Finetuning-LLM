from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull() & (F.col("balance") <= 63000) & (F.col("balance") < 71000))
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
    .orderBy("closed_count")
)
