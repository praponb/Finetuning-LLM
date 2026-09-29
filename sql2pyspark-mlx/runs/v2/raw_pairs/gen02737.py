from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("started_at") < "2024-11-01") & (F.col("browser") == "Chrome")) | (F.col("started_at") < "2025-07-09"))
    .orderBy("device")
)
