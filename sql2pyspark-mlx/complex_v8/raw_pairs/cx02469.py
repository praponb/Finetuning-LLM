from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_id = Window.partitionBy("track_id")

recent_plays = (
    df_plays
    .filter((F.col("source").isin("album", "search", "playlist")) & (F.col("device") != "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("source").isin("playlist", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group_2"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_windows = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "device_slug",
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.release_date") >= F.lit("2024-09-26").cast("date"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.device_slug",
        "s.played_at_seq",
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    with_parent
    .select("id", "played_at_seq", "ms_played")
    .unpivot(
        ids=["id"],
        values=["played_at_seq", "ms_played"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
