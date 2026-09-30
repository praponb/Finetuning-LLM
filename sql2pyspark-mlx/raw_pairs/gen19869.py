from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("has_fault", "id", "k")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k")

lines = (
    df_app_logs
    .select("id", "latency_ms", "status_code", F.explode("context").alias("k", "value"))
)

parsed = (
    lines
    .select(
        "id",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    parsed
    .filter(F.col("k").isin(["version", "region"]))
    .select(
        "*",
        F.lead("status_code_high").over(w).alias("next_status_code_high"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.row_number().over(w).alias("row_seq"),
        F.stddev_samp("status_code").over(w2).alias("k_stddev_samp_status_code"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    latest
    .filter(~F.col("k").eqNullSafe(F.lit("trace")))
    .select(
        "id",
        "status_code",
        "k",
        "has_fault",
        "next_status_code_high",
        "next_status_code",
        "row_seq",
        "rank_in_group",
        F.coalesce("k_stddev_samp_status_code", F.lit(0)).alias("k_stddev_samp_status_code_filled"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("next_status_code") + F.col("next_status_code_high")).alias("next_status_code_plus_next_status_code_high"),
        (F.col("has_fault") * F.col("next_status_code_high")).alias("has_fault_times_next_status_code_high"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

grouped = (
    filtered
    .groupBy(
        F.when(F.col("status_code") >= 396, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.round(F.avg("next_status_code_high"), 2).alias("avg_next_status_code_high"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
    )
)

result = (
    grouped
    .select(
        "status_code_tier",
        "version_count",
        "avg_next_status_code_high",
        "trace_count",
    )
    .orderBy("status_code_tier")
)
