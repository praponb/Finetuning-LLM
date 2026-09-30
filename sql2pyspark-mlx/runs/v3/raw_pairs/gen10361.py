from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select("device")
    .distinct()
    .orderBy("device")
    .limit(20)
)
