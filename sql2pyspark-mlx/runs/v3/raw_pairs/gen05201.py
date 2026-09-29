from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select("country", "started_at")
    .orderBy("started_at")
)
