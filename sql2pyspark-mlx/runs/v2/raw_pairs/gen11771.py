from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2025-07-01")
    .select("browser")
    .unionByName(df_sessions.filter(F.col("browser") == "Firefox").select("browser"))
    .distinct()
)
