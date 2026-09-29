from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("key", "host")
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.avg(F.when(F.col("key") == "region", F.col("latency_ms"))).alias("region_avg_latency_ms"),
    )
    .orderBy(F.col("latency_ms_bit_or").desc_nulls_first())
)
