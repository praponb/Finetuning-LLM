from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .withColumn("is_completed", F.when(F.col("status") == "COMPLETED", 1).otherwise(0))
)
