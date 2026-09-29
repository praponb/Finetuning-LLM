from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id", "mood").orderBy(F.col("max_release_date_dayofweek").desc(), F.col("id").desc(), F.col("mood_pos").desc())

scoped = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

per_label = (
    scoped
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_filled",
        "release_date_dayofweek",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

mood_stats = (
    per_label
    .groupBy("mood")
    .agg(F.max("release_date_dayofweek").alias("max_release_date_dayofweek"))
)

vs_group = (
    per_label.alias("s")
    .join(mood_stats.alias("g"), F.col("g.mood") == F.col("s.mood"), "inner")
    .filter(F.col("s.release_date_dayofweek") == F.col("g.max_release_date_dayofweek"))
    .select(
        "s.*",
        "g.max_release_date_dayofweek",
        F.round(F.col("s.release_date_dayofweek") - F.col("g.max_release_date_dayofweek"), 2).alias("release_date_dayofweek_minus_max"),
    )
)

unique_rows = (
    vs_group
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    unique_rows
    .select(
        "*",
        F.round(F.col("release_date_dayofweek") / F.nullif(F.col("duration_sec_filled"), F.lit(0)), 3).alias("release_date_dayofweek_per_duration_sec_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("mood")), " ", "_")).alias("mood_slug"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows = (
    labeled.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_filled",
        "release_date_dayofweek",
        "max_release_date_dayofweek",
        "release_date_dayofweek_minus_max",
        "release_date_dayofweek_per_duration_sec_filled",
        "mood_slug",
        "title_prefix",
        "track_count",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
    .orderBy("title_clean")
)
