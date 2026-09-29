from pyspark.sql import functions as F

result = (
    df_sessions
    .select("customer_id", "country", F.upper("device").alias("device_upper"))
    .orderBy("country", "device_upper")
)
