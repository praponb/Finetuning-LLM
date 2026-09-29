from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy("bpm", "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_bpm_moving_6 = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_latest = Window.partitionBy("artist_id").orderBy(F.col("prev2_bpm").desc(), F.col("id").desc())

filtered = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.lower(F.col("title")).like("%so%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.lag("bpm", 2).over(w_all_by_bpm).alias("prev2_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.max("duration_sec").over(w_is_explicit_by_bpm_moving_6).alias("duration_sec_max_7"),
    )
)

deduped = (
    windowed
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

artist_id_stats = (
    deduped
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

above_average = (
    deduped.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

artist_id_stats_2 = (
    above_average
    .groupBy("artist_id")
    .agg(F.avg("bpm_share_of_is_explicit_artist_id").alias("avg_bpm_share_of_is_explicit_artist_id"))
)

compared = (
    above_average.alias("s")
    .join(artist_id_stats_2.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_share_of_is_explicit_artist_id") > F.col("g.avg_bpm_share_of_is_explicit_artist_id"))
    .select(
        "s.*",
        "g.avg_bpm_share_of_is_explicit_artist_id",
        F.round(F.col("s.bpm_share_of_is_explicit_artist_id") - F.col("g.avg_bpm_share_of_is_explicit_artist_id"), 2).alias("bpm_share_of_is_explicit_artist_id_minus_avg"),
    )
)

exploded_detail = (
    compared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.prev2_bpm",
        "s.bpm_share_of_is_explicit_artist_id",
        "s.duration_sec_max_7",
        "s.bpm_minus_avg",
        "s.avg_bpm_share_of_is_explicit_artist_id",
        "s.bpm_share_of_is_explicit_artist_id_minus_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    exploded_detail
    .filter(F.col("play_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "moods",
        "bpm_share_of_is_explicit_artist_id",
        "duration_sec_max_7",
        "bpm_minus_avg",
        "avg_bpm_share_of_is_explicit_artist_id",
        "bpm_share_of_is_explicit_artist_id_minus_avg",
        "play_id",
        "play_played_at",
        "play_device",
        "play_is_skipped",
    )
    .orderBy(
        F.col("duration_sec_max_7").asc_nulls_last(),
        F.col("bpm_share_of_is_explicit_artist_id_minus_avg").desc_nulls_first(),
    )
)
