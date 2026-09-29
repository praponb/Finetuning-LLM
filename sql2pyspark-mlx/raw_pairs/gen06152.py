from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1819, "HIGH").otherwise("LOW"))
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 389, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["latency_ms_tier", "status_code_tier"], ["latency_ms_tier"], []],
        "latency_ms_tier",
        "status_code_tier",
    )
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .filter(F.col("num_levels") < 1000)
    .orderBy("latency_ms_tier")
)
