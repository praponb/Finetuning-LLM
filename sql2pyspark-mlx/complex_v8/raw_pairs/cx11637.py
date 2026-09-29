from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_latest = Window.partitionBy("artist_id").orderBy(F.col("track_count").desc(), F.col("id").desc())

base_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 10)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

latest_per_key = (
    base_tracks
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.max("release_date").alias("last_track_release_date"),
        F.max("release_date").alias("last_track_release_date_2"),
    )
)

with_counts = (
    latest_per_key.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.last_track_release_date",
        "t.last_track_release_date_2",
    )
)

latest_per_key_2 = (
    with_counts
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent = (
    latest_per_key_2.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Pop"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.title_slug",
        "s.release_date_label",
        "s.track_count",
        "s.last_track_release_date_2",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

deduped = (
    with_parent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

artist_id_stats = (
    deduped
    .groupBy("artist_id")
    .agg(F.avg("track_count").alias("avg_track_count"))
)

above_average = (
    deduped.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.track_count") > F.col("g.avg_track_count"))
    .select(
        "s.*",
        "g.avg_track_count",
        F.round(F.col("s.track_count") - F.col("g.avg_track_count"), 2).alias("track_count_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "duration_sec",
        "track_count",
        "bpm",
    )
    .unpivot(
        ids=["id"],
        values=["duration_sec", "track_count", "bpm"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
