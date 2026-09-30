from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_n_rows = Window.partitionBy("listener_id", "n_rows")
w_track_id_by_total_ms_played = Window.partitionBy("track_id").orderBy(F.col("total_ms_played_filled_2").desc_nulls_first(), "track_id", "listener_id", "source").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled_2"),
    )
)

totals = (
    base
    .groupBy("track_id", "listener_id", "source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ms_played_filled_2").alias("total_ms_played_filled_2"),
    )
)

qualified = (
    totals.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")) & (~F.col("p.is_skipped")), "left_semi")
)

filtered = (
    qualified.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")) & (F.col("p.track_id").isNotNull()), "left_semi")
)

metrics = (
    filtered
    .select(
        "track_id",
        "listener_id",
        "source",
        "n_rows",
        "total_ms_played_filled_2",
        F.min("total_ms_played_filled_2").over(w_listener_id_n_rows).alias("listener_id_n_rows_min_total_ms_played_filled_2"),
        F.count(F.lit(1)).over(w_track_id_by_total_ms_played).alias("running_count"),
    )
)

with_parent_track = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.track_id",
        "s.listener_id",
        "s.source",
        "s.n_rows",
        "s.running_count",
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_parent_track
    .select(
        "track_id",
        "listener_id",
        "n_rows",
        "running_count",
        "track_title",
        "track_moods",
        "track_bpm",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        F.upper(F.trim(F.col("track_title"))).alias("track_title_clean"),
    )
    .orderBy("listener_id")
)
