from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .withColumn("device_upper", F.upper("device"))
    .withColumn("device_lower", F.lower("device"))
)
