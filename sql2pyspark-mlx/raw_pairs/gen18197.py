from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode("context").alias("attr_key", "v"))
)

result = (
    exploded
    .groupBy("attr_key", "host")
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
    )
)
