from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").asc_nulls_first())
w2 = Window.partitionBy("attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    lines
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    unpacked
    .filter(F.col("attr_key").isNotNull())
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        "flag_bit2",
        "status_code_high",
        F.greatest("latency_ms", "status_code").alias("greatest_latency_ms_status_code"),
    )
)

scored = (
    base
    .filter(F.col("greatest_latency_ms_status_code") <= 27)
    .select(
        "latency_ms",
        "status_code",
        "flag_bit2",
        "greatest_latency_ms_status_code",
        F.dense_rank().over(w).alias("status_code_high_dense_rank"),
        F.min("status_code_high").over(w2).alias("attr_key_min_status_code_high"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("flag_bit2") >= 1, "LARGE").otherwise("SMALL").alias("flag_bit2_tier"),
        F.when(F.col("attr_key_min_status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("attr_key_min_status_code_high_tier"),
    )
    .agg(
        F.max("attr_key_min_status_code_high").alias("max_attr_key_min_status_code_high"),
        F.avg("attr_key_min_status_code_high").alias("avg_attr_key_min_status_code_high"),
        F.percentile_approx("flag_bit2", 0.5).alias("flag_bit2_p50"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
    .filter(F.col("flag_bit2_p50") >= 0)
)
