from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & F.col("customer_id").isin([6, 35]))
    .select("country", "device", "customer_id")
)
