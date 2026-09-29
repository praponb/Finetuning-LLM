from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari", "Firefox"]))
    .select("device")
    .unionByName(df_sessions.filter(F.col("device") == "mobile").select("device"))
    .distinct()
)
