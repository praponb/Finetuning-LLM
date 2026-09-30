from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_played_at_to_date = Window.partitionBy(F.to_date("played_at"))

prepared_plays = (
    df_plays
    .filter((F.col("ms_played") > 160264) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_activity = (
    prepared_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
    .filter(F.col("ms_played") <= 301149)
)

derived = (
    with_activity
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "days_since_played_at",
        F.first("played_at").over(w_listener_id_by_ms_played_ful).alias("first_played_at"),
        F.row_number().over(w_all_by_ms_played).alias("seq_no"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_to_date), F.lit(0)), 4).alias("ms_played_share_of_played_at_to_date"),
    )
)

top_per_group = with_windows.filter(F.col("seq_no") <= 5)

filtered = (
    top_per_group.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.bpm") > 76), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "days_since_played_at",
        "seq_no",
        F.round(F.col("ms_played") / F.nullif(F.col("days_since_played_at"), F.lit(0)), 3).alias("ms_played_per_days_since_played_at"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
    .orderBy(F.col("track_id").asc(), "days_since_played_at")
)
