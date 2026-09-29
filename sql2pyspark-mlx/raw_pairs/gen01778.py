from pyspark.sql import functions as F

result = (
    df_app_logs
    .select("id", "log_ts", F.explode("context").alias("attr_key", "attr_value"))
)
