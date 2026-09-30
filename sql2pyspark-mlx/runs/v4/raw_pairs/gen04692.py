from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode("context").alias("k", "value"))
)

result = flattened.select("id", "log_ts", "k", "value")
