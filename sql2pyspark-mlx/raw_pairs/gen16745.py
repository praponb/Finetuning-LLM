from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & (F.to_date("started_at") >= "2025-07-01"))
    .select("device")
    .orderBy(F.col("device").desc())
)
