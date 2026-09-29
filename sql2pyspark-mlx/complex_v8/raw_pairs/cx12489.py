from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("duration_sec").between(154, 409)) | (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

artist_id_stats = (
    valid
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

outliers = (
    valid.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

grouped = (
    outliers
    .groupBy("is_explicit", "release_date_quarter", "artist_id")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum("avg_bpm").alias("total_avg_bpm"),
        F.min("bpm_minus_avg").alias("min_bpm_minus_avg"),
        F.count("*").alias("cnt"),
    )
)

filtered = (
    grouped.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 100) & (F.col("t.is_explicit")), "left_semi")
)

qualified = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") == "BR") & (F.lower(F.col("a.artist_name")).like("%un%")), "left_anti")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

with_stats = (
    qualified.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.first_track_release_date")
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("bpm").alias("min_track_bpm"))
)

with_counts = (
    with_stats.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.min_track_bpm")
)

result = (
    with_counts
    .select(
        "is_explicit",
        "release_date_quarter",
        "artist_id",
        "title_list",
        "total_avg_bpm",
        "min_bpm_minus_avg",
        "first_track_release_date",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.date_format(F.col("first_track_release_date"), "MMM yyyy").alias("first_track_release_date_label"),
    )
    .orderBy(
        F.col("cnt_bucket").desc_nulls_first(),
        F.col("release_date_quarter").desc(),
    )
)
