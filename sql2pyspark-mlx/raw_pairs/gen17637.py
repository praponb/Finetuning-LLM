from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("started_at_hour", F.hour("started_at"))
    .orderBy(F.col("customer_id").desc(), "device")
)
