from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Edge", "Firefox"]))
    .select("browser")
    .unionByName(df_sessions.filter(F.col("country") == "DE").select("browser"))
)
