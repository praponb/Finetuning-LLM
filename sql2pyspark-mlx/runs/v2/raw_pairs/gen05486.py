from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select("device")
    .intersect(df_sessions.filter(F.col("country") == "TH").select("device"))
)
