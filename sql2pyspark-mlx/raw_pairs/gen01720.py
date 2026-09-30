from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("key", "service").orderBy("latency_ms")
w3 = Window.partitionBy("level")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

kept_keys = (
    exploded
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("level") != "DEBUG")
        .select("service")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("latency_ms_rank", F.rank().over(w))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w2))
    .withColumn("level_count_latency_ms", F.count("latency_ms").over(w3))
)

base = (
    with_windows
    .filter((~F.col("key").eqNullSafe(F.lit("trace"))) & F.col("key").isNotNull())
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

filtered = (
    base
    .filter(F.col("latency_ms_percent_rank") > 0.71)
    .select(
        "id",
        "service",
        "level",
        "value",
        "latency_ms_rank",
        "latency_ms_percent_rank",
        "level_count_latency_ms",
        "clean_level",
        "key_filled",
        F.lower("key").alias("key_lower"),
    )
)

result = (
    filtered
    .select(
        "id",
        "level",
        "latency_ms_rank",
        "latency_ms_percent_rank",
        "level_count_latency_ms",
        "clean_level",
        "key_filled",
        "key_lower",
        F.when(F.col("level").rlike("^(DEBUG|INFO)$"), "GROUP_B").when(F.col("level").rlike("^(WARN|ERROR)$"), "CORE").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("level").asc_nulls_last(), F.col("level_count_latency_ms").desc())
)
