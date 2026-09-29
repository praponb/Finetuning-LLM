from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k_max_latency_ms_tier").orderBy(F.col("num_rows").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

unpacked = (
    lines
    .filter(F.col("k").isin(["region", "trace", "version"]))
    .select(
        "id",
        "latency_ms",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("k") != "version")
    .select("*", F.length("k").alias("k_length"))
)

valid = (
    filtered
    .filter(F.col("status_code_high") > 1)
    .withColumn("status_code_high_filled", F.coalesce("status_code_high", F.lit(0)))
)

cleaned = (
    valid
    .filter(F.col("has_fault").isNotNull())
    .select(
        "id",
        "latency_ms",
        "k",
        "value",
        "status_code_high",
        "k_length",
        "status_code_high_filled",
        F.least("status_code_high", "has_fault").alias("least_status_code_high_has_fault"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "latency_ms",
        "k_length",
        "status_code_high_filled",
        "least_status_code_high_has_fault",
        F.count("status_code_high").over(w).alias("k_count_status_code_high"),
        F.max("latency_ms").over(w).alias("k_max_latency_ms"),
        F.avg("latency_ms").over(w).alias("k_avg_latency_ms"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("k_max_latency_ms") >= 2217, "LARGE").otherwise("SMALL").alias("k_max_latency_ms_tier"),
        F.when(F.col("least_status_code_high_has_fault") >= 84, "HIGH").otherwise("LOW").alias("least_status_code_high_has_fault_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("k_length"), 2).alias("avg_k_length"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count("*").alias("cnt"),
        F.round(F.avg("k_avg_latency_ms"), 2).alias("avg_k_avg_latency_ms"),
        F.sum("latency_ms").alias("total_latency_ms"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("avg_k_length").asc_nulls_first())
)
