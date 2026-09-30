from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .groupBy("host")
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
    )
    .filter(F.col("status_code_p90") > 0)
    .orderBy(F.col("status_code_p90").desc())
)
