from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2024-12-09") & F.col("customer_id").isin([9, 27]))
    .select("id", "device")
)
