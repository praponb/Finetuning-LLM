from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date_plus_90d").desc(), F.col("id").desc())

recent = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 90).alias("release_date_plus_90d"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

deduped = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

artist_id_stats = (
    deduped
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

compared = (
    deduped.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

enriched_artist = (
    compared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.days_since_release_date",
        "s.avg_bpm",
        "s.bpm_minus_avg",
        "a.artist_name",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

profiled = (
    enriched_artist.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    profiled
    .filter(F.col("days_since_release_date") < 525)
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "avg_bpm",
        "bpm_minus_avg",
        "artist_name",
        "play_count",
        "play_count_2",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("days_since_release_date").cast("string")).alias("artist_name_ref"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("artist_name").asc_nulls_last())
)
