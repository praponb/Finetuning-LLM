from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 1500)
    .withColumn("method_status_key", F.concat_ws("/", "method", "status"))
    .withColumn("is_failed", F.when(F.col("status") == "FAILED", 1).otherwise(0))
)
