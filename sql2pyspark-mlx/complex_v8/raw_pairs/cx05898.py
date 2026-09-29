from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_year = Window.orderBy(F.col("played_at_year").desc_nulls_first(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_year_by_played_at = Window.partitionBy("played_at_year").orderBy("played_at", "id")
w_dedup = Window.partitionBy("track_id", "listener_id").orderBy(F.col("is_skipped_min_played_at_year").desc(), F.col("id").desc())

cleaned_plays = (
    df_plays
    .filter(F.col("track_id") >= 9)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.year(F.col("played_at")).alias("played_at_year"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

metrics = (
    cleaned_plays
    .select(
        "*",
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_all_by_played_at_year)).alias("ms_played_bucket_change"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_is_skipped), F.lit(0)), 4).alias("ms_played_bucket_share_of_is_skipped"),
        F.min("played_at_year").over(w_is_skipped).alias("is_skipped_min_played_at_year"),
        F.round(F.cume_dist().over(w_played_at_year_by_played_at), 3).alias("played_at_cume_dist"),
    )
)

labeled = metrics.withColumn("is_skipped_flag", F.when(F.col("is_skipped"), 1).otherwise(0))

enriched_track = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(~F.col("t.is_explicit"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.ms_played_bucket",
        "s.ms_played_bucket_change",
        "s.ms_played_bucket_share_of_is_skipped",
        "s.is_skipped_min_played_at_year",
        "s.played_at_cume_dist",
        "s.is_skipped_flag",
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

unique_rows = (
    enriched_track
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details = (
    unique_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.ms_played_bucket",
        "s.ms_played_bucket_change",
        "s.played_at_cume_dist",
        "s.is_skipped_flag",
        "s.track_title",
        "s.track_is_explicit",
        "s.listener_plan",
        "s.listener_username",
        F.col("t.is_explicit").alias("track_is_explicit_2"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.moods").alias("track_moods"),
    )
)

result = (
    with_details
    .cube("track_artist_id", "listener_id")
    .agg(
        F.sum(F.when(F.col("track_is_explicit_2"), 1).otherwise(0)).alias("n_track_is_explicit_2_true"),
        F.min("played_at").alias("first_played_at"),
    )
    .orderBy(
        F.col("track_artist_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
