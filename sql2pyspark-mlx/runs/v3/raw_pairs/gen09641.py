from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
    .withColumn("clean_method", F.trim("method"))
)
