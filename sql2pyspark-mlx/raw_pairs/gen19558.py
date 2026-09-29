from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("attr_key", "v"))
)

result = lines.select("id", "level", "attr_key")
