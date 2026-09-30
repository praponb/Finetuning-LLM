from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())

filtered_plays = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

deduped = (
    filtered_plays
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

source_stats = (
    deduped
    .groupBy("source")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

above_average = (
    deduped.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

result = (
    above_average
    .groupingSets(
        [["listener_id", "device", "is_skipped"], ["listener_id"]],
        "listener_id", "device", "is_skipped",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("ms_played_minus_max"), 2).alias("stddev_ms_played_minus_max"),
    )
    .select(
        "listener_id",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "is_skipped",
        "gid",
        "row_count",
        "stddev_ms_played_minus_max",
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
        F.col("is_skipped").asc_nulls_last(),
    )
)
