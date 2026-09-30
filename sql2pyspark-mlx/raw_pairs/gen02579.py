from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms_rounded").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

base = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("service") == "search"))
    .withColumn("latency_ms_rounded", F.round("latency_ms", 1))
    .withColumn("resolved_key", F.coalesce("key", F.lit("DIRECT")))
)

filtered = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "latency_ms_rounded",
        "resolved_key",
        F.when(F.col("latency_ms") >= 2284, "High").when(F.col("latency_ms") >= 431, "Medium").otherwise("Low").alias("latency_ms_level"),
        F.floor("latency_ms_rounded").alias("latency_ms_rounded_whole"),
    )
)

base_app_logs = (
    filtered
    .filter(F.col("latency_ms_rounded") > 69)
    .select(
        "id",
        "latency_ms_rounded",
        "latency_ms_level",
        (F.col("latency_ms_rounded_whole") - F.col("latency_ms_rounded")).alias("latency_ms_rounded_whole_minus_latency_ms_rounded"),
        F.coalesce("latency_ms_rounded", F.lit(0)).alias("latency_ms_rounded_filled"),
    )
)

scored = (
    base_app_logs
    .filter(F.col("latency_ms_rounded").between(34, 47))
    .withColumn("latency_ms_rounded_rank", F.rank().over(w))
)

report = (
    scored
    .select(
        "id",
        "latency_ms_rounded",
        "latency_ms_level",
        "latency_ms_rounded_whole_minus_latency_ms_rounded",
        "latency_ms_rounded_filled",
    )
    .orderBy(F.col("id").desc())
)

result = report.select("latency_ms_rounded", "latency_ms_level", "latency_ms_rounded_filled")
