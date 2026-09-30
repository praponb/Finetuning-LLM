from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2314, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("status_code_per_error"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.sum(F.when(F.col("key") == "trace", F.col("latency_ms")).otherwise(0)).alias("trace_latency_ms"),
    )
    .orderBy(F.col("trace_latency_ms").asc_nulls_first())
)
