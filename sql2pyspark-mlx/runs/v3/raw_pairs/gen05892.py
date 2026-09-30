from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "country", "started_at")
    .orderBy("started_at", "id")
)
