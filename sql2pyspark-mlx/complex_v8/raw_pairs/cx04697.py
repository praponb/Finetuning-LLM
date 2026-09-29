from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-28").cast("date")) & (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

artist_id_stats = (
    scoped_tracks
    .groupBy("artist_id")
    .agg(F.avg("least_bpm_duration_sec").alias("avg_least_bpm_duration_sec"))
)

vs_group = (
    scoped_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.least_bpm_duration_sec") > F.col("g.avg_least_bpm_duration_sec"))
    .select(
        "s.*",
        "g.avg_least_bpm_duration_sec",
        F.round(F.col("s.least_bpm_duration_sec") - F.col("g.avg_least_bpm_duration_sec"), 2).alias("least_bpm_duration_sec_minus_avg"),
    )
)

artist_id_stats_2 = (
    vs_group
    .groupBy("artist_id")
    .agg(F.avg("least_bpm_duration_sec_minus_avg").alias("avg_least_bpm_duration_sec_minus_avg"))
)

outliers = (
    vs_group.alias("s")
    .join(artist_id_stats_2.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.least_bpm_duration_sec_minus_avg") > F.col("g.avg_least_bpm_duration_sec_minus_avg"))
    .select(
        "s.*",
        "g.avg_least_bpm_duration_sec_minus_avg",
        F.round(F.col("s.least_bpm_duration_sec_minus_avg") - F.col("g.avg_least_bpm_duration_sec_minus_avg"), 2).alias("least_bpm_duration_sec_minus_avg_minus_avg"),
    )
)

result = (
    outliers
    .select("id", "duration_sec_bucket", "duration_sec")
    .unpivot(
        ids=["id"],
        values=["duration_sec_bucket", "duration_sec"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
