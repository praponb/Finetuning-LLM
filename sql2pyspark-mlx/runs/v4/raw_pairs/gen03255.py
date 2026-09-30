from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("attr_key") == "trace", F.col("status_code"))).alias("trace_total_status_code"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy("level")
)
