from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_listener_birth_year_p = Window.orderBy(F.col("listener_birth_year").desc(), "id").rowsBetween(-2, -1)
w_all_by_listener_birth_year_f = Window.orderBy(F.col("listener_birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_trac = Window.partitionBy("track_id", "listener_id").orderBy(F.col("track_bpm").asc_nulls_last(), "id")
w_played_at_month = Window.partitionBy(F.month("played_at"))
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_all_by_listener_birth_year_r = Window.orderBy(F.col("listener_birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_listener_birth_year = Window.orderBy(F.col("listener_birth_year").desc(), "id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_listener_birth_year_t = Window.orderBy(F.col("listener_birth_year").desc())

cleaned = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")) & (F.col("ms_played") <= 324058))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

combined_details_listener = (
    cleaned.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.source_clean",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

windowed = (
    combined_details_listener
    .select(
        "*",
        F.max("track_bpm").over(w_all_by_listener_birth_year_p).alias("track_bpm_max_2_prior"),
        F.last("played_at").over(w_all_by_listener_birth_year_f).alias("last_played_at"),
        F.lag("played_at").over(w_track_id_listener_id_by_trac).alias("prev_played_at"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_played_at_month), F.lit(0)), 4).alias("listener_birth_year_share_of_played_at_month"),
        F.sum("track_bpm").over(w_listener_id_source).alias("listener_id_source_sum_track_bpm"),
        F.first("played_at", ignorenulls=True).over(w_all_by_listener_birth_year_r).alias("first_known_played_at"),
        F.first("track_bpm").over(w_all_by_listener_birth_year_f).alias("first_track_bpm"),
        F.row_number().over(w_all_by_listener_birth_year).alias("listener_birth_year_seq"),
        F.sum("track_bpm").over(w_played_at_month).alias("played_at_month_sum_track_bpm"),
        (F.col("ms_played") - F.lag("ms_played").over(w_track_id_listener_id_by_trac)).alias("ms_played_change"),
        F.row_number().over(w_track_id_listener_id_by_trac).alias("track_bpm_seq"),
        F.rank().over(w_track_id_by_ms_played).alias("ms_played_rank"),
        F.round(F.percent_rank().over(w_all_by_listener_birth_year_t), 3).alias("listener_birth_year_percent_rank"),
    )
)

scored = (
    windowed
    .select(
        "*",
        (F.floor(F.col("first_track_bpm") / 25) * 25).alias("first_track_bpm_bucket"),
    )
)

result = (
    scored
    .select("track_bpm")
    .distinct()
    .orderBy("track_bpm")
)
