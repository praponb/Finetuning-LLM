from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2024-05-01")
    .select("device")
    .unionByName(df_sessions.filter(F.col("country") == "UK").select("device"))
    .orderBy("device")
)
