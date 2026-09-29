from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") != "Edge")
    .select("id", "device")
    .orderBy("device")
)
