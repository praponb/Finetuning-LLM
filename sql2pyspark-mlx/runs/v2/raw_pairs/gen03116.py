from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Edge") & (F.col("browser") == "Safari"))
    .select("id", "browser", "started_at")
    .orderBy("browser")
)
