from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_ms_played_pri = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, -1)
w_played_at_month = Window.partitionBy(F.month("played_at"))

scoped_plays = (
    df_plays
    .filter((F.col("track_id") >= 3) & (~F.col("device").isin("tv", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 239955, "A").when(F.col("ms_played") >= 73177, "B").otherwise("C").alias("ms_played_tier"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

with_windows = (
    scoped_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "device_prefix",
        F.lag("ms_played").over(w_listener_id_by_ms_played).alias("prev_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_by_ms_played_pri), 2).alias("ms_played_avg_2_prior"),
        F.min("ms_played").over(w_played_at_month).alias("played_at_month_min_ms_played"),
        F.sum("ms_played").over(w_played_at_month).alias("played_at_month_sum_ms_played"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        "s.ms_played_avg_2_prior",
        "s.played_at_month_sum_ms_played",
        F.col("t.moods").alias("track_moods"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "played_at",
        "ms_played",
        "device_prefix",
        "ms_played_avg_2_prior",
        "played_at_month_sum_ms_played",
        "track_moods",
        "artist_country",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.round(F.col("ms_played") / F.nullif(F.col("ms_played_avg_2_prior"), F.lit(0)), 3).alias("ms_played_per_ms_played_avg_2_prior"),
    )
    .orderBy(
        F.col("ms_played").asc_nulls_last(),
        F.col("played_at_month_sum_ms_played").desc(),
    )
)
