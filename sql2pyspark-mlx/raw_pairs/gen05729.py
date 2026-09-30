from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"))
    .filter(F.col("success_count") > 3)
)
