from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

result = lines.select("id", "level", "latency_ms", "k")
