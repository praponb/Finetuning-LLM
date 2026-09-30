from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_track_release_date_ru = Window.orderBy("track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_is_skipped_av = Window.partitionBy("listener_id").orderBy(F.col("is_skipped_avg_track_bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_is_skipped_av_2 = Window.partitionBy("listener_id").orderBy(F.col("is_skipped_avg_track_bpm").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

enriched_listener = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_label",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

sequenced = (
    enriched_listener
    .withColumn("is_skipped_avg_track_bpm", F.round(F.avg("track_bpm").over(w_is_skipped), 2))
    .withColumn("last_known_track_release_date", F.last("track_release_date", ignorenulls=True).over(w_all_by_track_release_date_ru))
)

tagged = (
    sequenced
    .filter(F.col("played_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "is_skipped_label",
        "track_release_date",
        "track_bpm",
        "is_skipped_avg_track_bpm",
        "last_known_track_release_date",
        F.date_trunc("month", F.col("listener_signup_date")).alias("listener_signup_date_month"),
        F.concat_ws(" / ", F.col("listener_country"), F.col("is_skipped_label")).alias("listener_country_is_skipped_label_label"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_semi")
)

with_windows = (
    without_matches
    .withColumn("first_track_bpm", F.first("track_bpm").over(w_listener_id_by_is_skipped_av))
    .withColumn("is_skipped_avg_track_bpm_max_2", F.max("is_skipped_avg_track_bpm").over(w_listener_id_by_is_skipped_av_2))
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "is_skipped_label",
        "is_skipped_avg_track_bpm",
        "last_known_track_release_date",
        "listener_signup_date_month",
        "listener_country_is_skipped_label_label",
        F.concat_ws("|", F.col("source"), F.col("is_skipped_label")).alias("source_is_skipped_label_label"),
    )
    .orderBy(
        F.col("is_skipped_label").desc(),
        "listener_country_is_skipped_label_label",
    )
)
