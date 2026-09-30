from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
    .filter(F.col("frozen_count") > 9)
    .orderBy(F.col("customer_id").desc(), F.col("frozen_count").desc())
    .limit(5)
)
