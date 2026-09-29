from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-07-27")
    .select("browser")
    .unionByName(df_sessions.filter(F.col("device") == "tablet").select("browser"))
    .orderBy("browser")
)
