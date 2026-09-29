from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "device", F.upper("country").alias("country_upper"))
)
