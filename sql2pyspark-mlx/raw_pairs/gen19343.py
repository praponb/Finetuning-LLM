from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id").isin([3, 34]) & (F.col("device") == "desktop")) | (F.col("started_at") < "2025-09-01"))
    .select("browser")
)
