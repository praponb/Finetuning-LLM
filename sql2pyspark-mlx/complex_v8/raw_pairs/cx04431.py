from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_is_explicit_duration_sec_den = Window.partitionBy("is_explicit", "duration_sec_dense_rank")
w_duration_sec_dense_rank_rele = Window.partitionBy("duration_sec_dense_rank", F.month("release_date")).orderBy("bpm")

filtered = df_tracks.filter((~F.col("is_explicit")) & (F.upper(F.trim(F.col("title"))).like("R%")))

metrics = (
    filtered
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.max("bpm").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_bpm"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
    )
)

filtered_2 = (
    metrics.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.hour(F.col("p.played_at")).between(9, 12)) & (F.col("p.is_skipped")), "left_anti")
)

metrics_2 = (
    filtered_2
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "artist_id_is_explicit_max_bpm",
        "duration_sec_dense_rank",
        F.sum("duration_sec").over(w_is_explicit_duration_sec_den).alias("is_explicit_duration_sec_dense_rank_sum_duration"),
        F.dense_rank().over(w_duration_sec_dense_rank_rele).alias("bpm_dense_rank"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    metrics_2.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_children = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.release_date",
        "s.is_explicit",
        "s.duration_sec_dense_rank",
        "s.first_played_at",
        "s.play_count",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

without_matches = (
    with_children.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (F.col("p.ms_played").between(56858, 239955)), "left_semi")
)

result = (
    without_matches
    .cube("play_id", "play_count", "play_is_skipped")
    .agg(
        F.round(F.stddev("duration_sec_dense_rank"), 2).alias("stddev_duration_sec_dense_rank"),
        F.max("release_date").alias("last_release_date"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("play_is_skipped").asc_nulls_last(),
    )
)
