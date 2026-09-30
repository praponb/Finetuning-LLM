from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc(), "id", "k")
w2 = Window.partitionBy("level_length_tier").orderBy(F.col("info_events").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

deduped = (
    exploded
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    deduped
    .select("host")
    .intersect(
        df_app_logs
        .filter(F.col("service").isin(["search", "auth", "checkout"]))
        .select("host")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("k").isNotNull() & (F.col("rn") == 1))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "value",
        "rn",
        F.trim("service").alias("clean_service"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

valid = (
    base
    .filter(F.col("service") != "search")
    .select(
        "service",
        "level",
        "latency_ms",
        "value",
        "rn",
        "k_filled",
        F.length("level").alias("level_length"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("level_length") >= 65, "PREMIUM").otherwise("STANDARD").alias("level_length_tier"),
        F.when(F.col("latency_ms") >= 2221, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count_if(F.col("level") == "INFO").alias("info_events"),
    )
    .withColumn("info_events_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("latency_ms_tier").asc_nulls_last())
)
