from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Firefox") & (F.col("country") == "JP"))
    .select("id", "browser", "device")
    .orderBy("device", F.col("id").desc())
    .limit(5)
)
