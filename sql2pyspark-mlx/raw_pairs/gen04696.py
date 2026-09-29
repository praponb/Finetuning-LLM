from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device").isin(["desktop", "mobile", "tablet"]) & (F.col("customer_id") == 19)) | F.col("customer_id").isin([17, 19, 28, 29]))
    .select("id", "browser", "started_at", "customer_id")
)
