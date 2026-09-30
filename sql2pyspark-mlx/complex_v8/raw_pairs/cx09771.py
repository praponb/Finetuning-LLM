from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_source_by_pct_is_skipped_fal = Window.partitionBy("source").orderBy(F.col("pct_is_skipped_false").desc())
w_source_by_pct_is_skipped_fal_2 = Window.partitionBy("source").orderBy(F.col("pct_is_skipped_false").desc(), "played_at_year", "source", "track_id")

prepared_plays = (
    df_plays
    .filter((F.col("ms_played").between(112786, 251066)) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.when(F.col("device").isin("desktop", "phone"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
    )
)

annotated = prepared_plays.select("*", F.date_trunc("day", F.col("played_at")).alias("played_at_day"))

per_group = (
    annotated
    .filter(F.col("listener_id") <= 12)
    .groupBy(F.year("played_at").alias("played_at_year"), "source", "track_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_false"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.sum("ms_played").alias("total_ms_played"),
        F.percentile_approx("played_at_dayofweek", 0.5).alias("median_played_at_dayofweek"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 73625)
    .drop("avg_ms_played_check")
)

windowed = (
    per_group
    .select(
        "played_at_year",
        "source",
        "track_id",
        "pct_is_skipped_false",
        "avg_ms_played",
        "total_ms_played",
        "median_played_at_dayofweek",
        F.round(F.col("pct_is_skipped_false") / F.nullif(F.sum("pct_is_skipped_false").over(w_source), F.lit(0)), 4).alias("pct_is_skipped_false_share_of_source"),
        F.rank().over(w_source_by_pct_is_skipped_fal).alias("pct_is_skipped_false_rank"),
        F.lag("avg_ms_played", 3).over(w_source_by_pct_is_skipped_fal_2).alias("prev3_avg_ms_played"),
    )
)

enriched_track = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.played_at_year",
        "s.source",
        "s.track_id",
        "s.pct_is_skipped_false",
        "s.avg_ms_played",
        "s.total_ms_played",
        "s.median_played_at_dayofweek",
        "s.pct_is_skipped_false_share_of_source",
        "s.pct_is_skipped_false_rank",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    enriched_track
    .filter(~F.col("artist_is_verified"))
    .select(
        "played_at_year",
        "track_id",
        "avg_ms_played",
        "total_ms_played",
        "median_played_at_dayofweek",
        "pct_is_skipped_false_share_of_source",
        "pct_is_skipped_false_rank",
        "track_artist_id",
        "track_duration_sec",
    )
    .orderBy(F.col("pct_is_skipped_false_share_of_source").desc_nulls_first())
)
