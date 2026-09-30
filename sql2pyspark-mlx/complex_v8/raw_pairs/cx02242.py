from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_source = Window.partitionBy("source")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_played_at_month_by_second_pl = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("second_played_at")
w_listener_id_second_played_at = Window.partitionBy("listener_id", F.to_date("second_played_at")).orderBy(F.col("source_min_days_since_played_at").desc(), "id")
w_listener_id_second_played_at_2 = Window.partitionBy("listener_id", F.to_date("second_played_at")).orderBy(F.col("source_min_days_since_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_plays = (
    df_plays
    .filter(F.col("ms_played") < 299017)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

metrics = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "days_since_played_at",
        "played_at_day",
        F.row_number().over(w_listener_id_by_ms_played).alias("rn"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_source), 2).alias("days_since_played_at_vs_source_avg"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        F.nth_value("played_at", 2).over(w_listener_id_by_ms_played_ful).alias("second_played_at"),
        F.min("days_since_played_at").over(w_source).alias("source_min_days_since_played_at"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "days_since_played_at",
        "days_since_played_at_vs_source_avg",
        "second_played_at",
        "source_min_days_since_played_at",
        F.round((F.unix_timestamp(F.col("second_played_at")) - F.unix_timestamp(F.col("played_at"))) / 3600, 1).alias("hours_played_at_to_second_played_at"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("played_at_day"))) / 3600, 1).alias("hours_played_at_day_to_played_at"),
        F.date_format(F.col("played_at_day"), "yyyy-MM-dd HH:00").alias("played_at_day_label"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_track_id), 2).alias("days_since_played_at_vs_track_id_avg"),
        F.rank().over(w_played_at_month_by_second_pl).alias("second_played_at_rank"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_second_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.nth_value("second_played_at", 2).over(w_listener_id_second_played_at_2).alias("second_second_played_at"),
        (F.col("days_since_played_at") - F.lag("days_since_played_at").over(w_listener_id_second_played_at)).alias("days_since_played_at_change"),
    )
)

combined_details_track = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .filter(F.size(F.col("t.moods")) >= 2)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source_min_days_since_played_at",
        "s.hours_played_at_to_second_played_at",
        "s.hours_played_at_day_to_played_at",
        "s.played_at_day_label",
        "s.days_since_played_at_vs_track_id_avg",
        "s.second_played_at_rank",
        "s.second_second_played_at",
        "s.days_since_played_at_change",
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "hours_played_at_to_second_played_at",
        "played_at_day_label",
        "days_since_played_at_vs_track_id_avg",
        "second_played_at_rank",
        "days_since_played_at_change",
        "track_duration_sec",
        "artist_country",
        "artist_debut_year",
    )
    .orderBy(F.col("track_id").asc(), "id")
    .limit(5)
)
