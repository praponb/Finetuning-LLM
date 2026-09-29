from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")

cleaned = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

flattened = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.explode("moods").alias("mood"),
    )
)

without_matches = (
    flattened.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date").isNotNull()), "left_anti")
    .filter(F.col("artist_id") <= 14)
)

with_trends = (
    without_matches
    .select(
        "*",
        F.size(F.collect_set("mood").over(w_artist_id)).alias("distinct_mood_in_artist_id"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
    )
)

scored = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "mood",
        "distinct_mood_in_artist_id",
        "duration_sec_vs_artist_id_avg",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.when(F.col("mood").isin("chill", "workout"), "group_a").when(F.col("mood") == "sad", "group_b").otherwise("other").alias("mood_group"),
    )
)

qualified = (
    scored
    .crossJoin(scored.agg(F.min("duration_sec").alias("overall_min_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_min_duration_sec"))
    .drop("overall_min_duration_sec")
)

artist_id_stats = (
    qualified
    .groupBy("artist_id")
    .agg(F.avg("bpm_bucket").alias("avg_bpm_bucket"))
)

above_average = (
    qualified.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_bucket") > F.col("g.avg_bpm_bucket"))
    .select(
        "s.*",
        "g.avg_bpm_bucket",
        F.round(F.col("s.bpm_bucket") - F.col("g.avg_bpm_bucket"), 2).alias("bpm_bucket_minus_avg"),
    )
)

result = (
    above_average
    .select("mood_group", "duration_sec_vs_artist_id_avg")
    .distinct()
    .orderBy(
        F.col("duration_sec_vs_artist_id_avg").desc(),
        F.col("mood_group").desc(),
    )
)
