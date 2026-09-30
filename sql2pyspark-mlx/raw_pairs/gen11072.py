from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "level", F.explode_outer("context").alias("k", "value"))
)

result = exploded.select("host", "k", "value")
