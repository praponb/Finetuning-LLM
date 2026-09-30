from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w_running = Window.orderBy(F.col("key_count_latency_ms").asc_nulls_first())

exploded = df_app_logs.select("id", "latency_ms", F.explode_outer("context").alias("key", "v"))

cleaned = (
    exploded
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "*",
        F.initcap(F.trim("key")).alias("key_title"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

enriched = (
    cleaned
    .filter(F.col("latency_ms") <= 1401)
    .select(
        "id",
        "latency_ms",
        "v",
        F.count("latency_ms").over(w).alias("key_count_latency_ms"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w_running).alias("key_count_latency_ms_percent_rank"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") != 1363)
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms").between(732, 1746))
        .select("id")
    )
)

reconciled_app_logs = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled_app_logs
    .filter(F.col("key_count_latency_ms") <= 16)
    .select(
        "id",
        "latency_ms",
        "v",
        "key_count_latency_ms",
        "key_count_latency_ms_percent_rank",
        (F.col("key_count_latency_ms_percent_rank") - F.col("latency_ms")).alias("key_count_latency_ms_percent_rank_minus_latency_ms"),
    )
)

result = (
    base
    .select(
        "latency_ms",
        "v",
        "key_count_latency_ms",
        "key_count_latency_ms_percent_rank",
        "key_count_latency_ms_percent_rank_minus_latency_ms",
    )
)
