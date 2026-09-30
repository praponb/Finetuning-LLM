from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_artist_id = Window.partitionBy("play_count", "artist_id")
w_artist_id_play_count_by_bpm = Window.partitionBy("artist_id", "play_count").orderBy(F.col("bpm").desc(), "id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("artist_id").orderBy(F.col("play_count_artist_id_min_play_count").desc(), F.col("id").desc())
w_play_count_is_explicit_by_pl = Window.partitionBy("play_count", "is_explicit").orderBy("play_count", "id").rowsBetween(-3, Window.currentRow)
w_play_count_is_explicit_by_pl_2 = Window.partitionBy("play_count", "is_explicit").orderBy("play_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
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

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    base_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

metrics = (
    profiled
    .select(
        "*",
        F.round(F.avg("bpm").over(w_play_count_artist_id), 2).alias("play_count_artist_id_avg_bpm"),
        F.ntile(3).over(w_artist_id_play_count_by_bpm).alias("bpm_tercile"),
        F.last("play_count", ignorenulls=True).over(w_artist_id_is_explicit_by_bpm).alias("last_known_play_count"),
        F.min("play_count").over(w_play_count_artist_id).alias("play_count_artist_id_min_play_count"),
    )
)

deduped = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

artist_id_stats = (
    deduped
    .groupBy("artist_id")
    .agg(F.avg("play_count").alias("avg_play_count"))
)

compared = (
    deduped.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.play_count") > F.col("g.avg_play_count"))
    .select(
        "s.*",
        "g.avg_play_count",
        F.round(F.col("s.play_count") - F.col("g.avg_play_count"), 2).alias("play_count_minus_avg"),
    )
)

windowed = (
    compared
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "play_count_artist_id_avg_bpm",
        "last_known_play_count",
        "avg_play_count",
        "play_count_minus_avg",
        F.round(F.avg("play_count_artist_id_avg_bpm").over(w_play_count_is_explicit_by_pl), 2).alias("play_count_artist_id_avg_bpm_avg_4"),
        F.first("play_count_minus_avg", ignorenulls=True).over(w_play_count_is_explicit_by_pl_2).alias("first_known_play_count_minus_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "play_count_artist_id_avg_bpm",
        "last_known_play_count",
        "avg_play_count",
        "play_count_minus_avg",
        "play_count_artist_id_avg_bpm_avg_4",
        "first_known_play_count_minus_avg",
        F.greatest(F.col("duration_sec"), F.col("avg_play_count")).alias("greatest_duration_sec_avg_play_count"),
        F.coalesce(F.col("last_known_play_count"), F.lit(0)).alias("last_known_play_count_filled"),
    )
    .orderBy(F.col("title").asc_nulls_last(), "artist_id")
)
