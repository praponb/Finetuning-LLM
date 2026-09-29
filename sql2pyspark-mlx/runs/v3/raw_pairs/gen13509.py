from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Firefox") & F.col("customer_id").isin([16, 28, 39]))
    .select("country", F.upper("device").alias("device_upper"))
)
