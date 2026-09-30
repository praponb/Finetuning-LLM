from pyspark.sql import functions as F

lines = df_app_logs.select("id", "log_ts", "level", F.explode("context").alias("k", "v"))

result = (
    lines
    .cube("k")
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.grouping("k").alias("gid"),
    )
    .orderBy(F.col("region_events").desc_nulls_first())
)
