from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_track_id_n_rows_by_n_rows_ru = Window.partitionBy("track_id", "n_rows").orderBy(F.col("n_rows").desc(), "track_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_plays

filtered_2 = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isin("radio", "album")) & (F.hour(F.col("p.played_at")).between(6, 8)), "left_anti")
)

rollup_base = (
    filtered_2
    .groupBy("track_id")
    .agg(
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.count("*").alias("n_rows"),
    )
)

enriched_track = (
    rollup_base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.track_id",
        "s.n_rows",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
    )
)

metrics = (
    enriched_track
    .select(
        "*",
        F.min("track_duration_sec").over(w_track_id).alias("track_id_min_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id_n_rows_by_n_rows_ru).alias("running_count"),
    )
)

result = (
    metrics
    .select(
        "track_id",
        "track_bpm",
        "track_duration_sec",
        "track_moods",
        "track_id_min_track_duration_sec",
        F.size(F.col("track_moods")).alias("track_moods_count"),
        F.size(F.col("track_moods")).alias("track_moods_count_2"),
    )
    .orderBy("track_duration_sec", F.col("track_id").asc())
    .limit(5)
)
