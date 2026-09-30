from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "mobile", "tablet"]) & (F.col("started_at") >= "2024-10-12"))
)
