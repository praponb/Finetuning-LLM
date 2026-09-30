from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

prepared = (
    df_tracks
    .filter((F.col("bpm") <= 155) & (F.length(F.trim(F.col("title"))) >= 6))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

qualified = (
    prepared
    .crossJoin(prepared.agg(F.avg("duration_sec_plus_bpm").alias("overall_avg_duration_sec_plus_bpm")))
    .filter(F.col("duration_sec_plus_bpm") > F.col("overall_avg_duration_sec_plus_bpm"))
    .drop("overall_avg_duration_sec_plus_bpm")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded_detail_plays = (
    deduped.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.duration_sec_plus_bpm",
        "s.title_ref",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

without_matches = (
    exploded_detail_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.play_listener_id")) & (F.col("l.plan") == "family") & (F.size(F.col("l.interests")) >= 2), "left_semi")
)

enriched_play = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.play_id",
        "s.play_listener_id",
        "s.play_played_at",
        "s.play_is_skipped",
        F.col("p.listener_id").alias("play_listener_id_2"),
    )
)

result = (
    enriched_play
    .rollup("play_listener_id_2", "artist_id")
    .agg(
        F.grouping("play_listener_id_2").alias("is_total_play_listener_id_2"),
        F.count("*").alias("cnt"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
    .orderBy(
        F.col("play_listener_id_2").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
