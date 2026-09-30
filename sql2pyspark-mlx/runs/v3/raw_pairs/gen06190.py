from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select("started_at")
)
