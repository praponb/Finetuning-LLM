from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2131, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["latency_ms_tier", "attr_key"], ["latency_ms_tier"], []],
        "latency_ms_tier",
        "attr_key",
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.sum("latency_ms").alias("total_latency_ms"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("min_latency_ms").desc())
)
