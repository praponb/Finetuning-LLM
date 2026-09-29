from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "browser", "device", F.lower("browser").alias("browser_lower"))
    .orderBy("browser", "id")
    .limit(20)
)
