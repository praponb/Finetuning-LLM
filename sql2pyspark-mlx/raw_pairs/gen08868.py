from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select("browser", "device")
    .orderBy("device")
)
