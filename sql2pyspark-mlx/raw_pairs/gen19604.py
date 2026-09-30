from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("started_at") >= "2025-09-09") & (F.col("country") != "SG") & (F.col("started_at") >= "2023-07-01")))
    .select("id", "started_at", "browser")
)
