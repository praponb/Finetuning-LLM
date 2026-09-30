from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id", "source").orderBy(F.col("played_at_plus_2h").desc(), F.col("id").desc())
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")

cleaned_plays = (
    df_plays
    .filter(F.col("ms_played") > 72464)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
    )
)

listener_id_stats = (
    cleaned_plays
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    cleaned_plays.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

deduped = (
    compared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    deduped
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "device_ref",
        "played_at_plus_2h",
        "avg_ms_played",
        "ms_played_minus_avg",
        F.min("ms_played").over(w_track_id).alias("track_id_min_ms_played"),
        F.sum("avg_ms_played").over(w_track_id).alias("track_id_sum_avg_ms_played"),
        F.lead("played_at", 2).over(w_all_by_ms_played).alias("next2_played_at"),
        F.size(F.collect_set("source").over(w_track_id)).alias("distinct_source_in_track_id"),
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
    )
)

totals = (
    sequenced
    .filter(F.col("avg_ms_played").between(64250.0, 242000.0))
    .groupBy(
        F.date_trunc("month", "played_at_plus_2h").alias("played_at_plus_2h_month"),
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.round(F.sum("ms_played") / F.nullif(F.sum("ms_played_tercile"), F.lit(0)), 3).alias("ms_played_to_ms_played_tercile_ratio"),
        F.round(F.sum("avg_ms_played"), 2).alias("total_avg_ms_played"),
        F.max("next2_played_at").alias("last_next2_played_at"),
        F.sum(F.when(F.col("is_skipped"), F.col("ms_played"))).alias("ms_played_is_skipped_true"),
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played_minus_avg").alias("total_ms_played_minus_avg"),
    )
)

result = (
    totals
    .select(
        "played_at_plus_2h_month",
        "played_at_month",
        "last_next2_played_at",
        "ms_played_is_skipped_true",
        "last_played_at",
        "total_ms_played_minus_avg",
    )
    .orderBy("last_played_at")
)
