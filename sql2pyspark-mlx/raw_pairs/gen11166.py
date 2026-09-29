from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy("attr_key")
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
    )
    .filter(F.col("num_rows") < 1000)
)
