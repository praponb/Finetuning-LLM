from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_device_played_at_month_by_ms = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_played_at_month_by_ms_2 = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(~F.col("source").isin("album", "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

unique_rows = (
    cleaned_plays
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    unique_rows
    .withColumn("last_played_at", F.last("played_at").over(w_device_played_at_month_by_ms))
    .withColumn("last_known_played_at", F.last("played_at", ignorenulls=True).over(w_device_played_at_month_by_ms_2))
)

enriched = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.last_played_at",
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ms_played",
        "is_skipped",
        "last_played_at",
        "track_moods",
        "track_artist_id",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
    .orderBy(F.col("ms_played").desc(), F.col("track_artist_id").desc_nulls_first())
)
