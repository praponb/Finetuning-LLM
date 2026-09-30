from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_release_date_mont = Window.partitionBy("play_count", F.date_trunc("month", "release_date")).orderBy(F.col("release_date_plus_30d").asc_nulls_last(), "id", "play_id").rowsBetween(-3, Window.currentRow)
w_play_count_release_date_mont_2 = Window.partitionBy("play_count", F.date_trunc("month", "release_date")).orderBy(F.col("release_date_plus_30d").asc_nulls_last(), "id", "play_id")
w_artist_id_play_count_by_rele = Window.partitionBy("artist_id", "play_count").orderBy(F.col("release_date_plus_7d").desc())
w_artist_id_play_count_by_rele_2 = Window.partitionBy("artist_id", "play_count").orderBy(F.col("release_date_plus_7d").desc(), "id", "play_id")

cleaned_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-10-23").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.abs(F.col("duration_sec") - 183).alias("duration_sec_distance"),
    )
)

exploded_detail_plays = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") < 218129), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

enriched = (
    exploded_detail_plays
    .select(
        "*",
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("M%")), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

summarized_rows = (
    qualified.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

windowed = (
    summarized_rows
    .select(
        "id",
        "bpm",
        "play_id",
        "play_source",
        "play_ms_played",
        "release_date_plus_30d",
        "release_date_plus_7d",
        F.round(F.avg("bpm_bucket").over(w_play_count_release_date_mont), 2).alias("bpm_bucket_avg_4"),
        F.lead("play_ms_played").over(w_play_count_release_date_mont_2).alias("next_play_ms_played"),
        F.rank().over(w_artist_id_play_count_by_rele).alias("release_date_plus_7d_rank"),
        F.row_number().over(w_artist_id_play_count_by_rele_2).alias("seq_no"),
    )
)

with_details_play = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (F.hour(F.col("p.played_at")).between(9, 12)), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("p.track_id"), "left")
    .select(
        "s.id",
        "s.play_id",
        "s.play_source",
        "s.play_ms_played",
        "s.release_date_plus_30d",
        "s.release_date_plus_7d",
        "s.next_play_ms_played",
        "s.release_date_plus_7d_rank",
        "s.seq_no",
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source_2"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    with_details_play
    .select(
        "id",
        "play_id",
        F.col("next_play_ms_played").alias("m_next_play_ms_played"),
        F.col("release_date_plus_7d_rank").alias("release_date_plus_7d_rank_v"),
        F.col("seq_no").alias("m_seq_no"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["m_next_play_ms_played", "release_date_plus_7d_rank_v", "m_seq_no"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
