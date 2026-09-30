from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(6, 10))
    .select("country", "device", "started_at")
    .orderBy("device", F.col("started_at").desc())
)
