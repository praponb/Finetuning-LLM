from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("customer_id").isin([34, 36, 39]) & (F.col("customer_id") == 36)))
    .withColumn("clean_device", F.trim("device"))
)
