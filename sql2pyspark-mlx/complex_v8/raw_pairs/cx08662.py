from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 94)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

with_details = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("N%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.title_ref",
        "a.artist_name",
    )
)

rollup_base = (
    with_details
    .groupBy("artist_id")
    .agg(
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("artist_name"))).like("S%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_artist_name_like"),
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.sum("bpm").alias("total_bpm"),
        F.min("bpm").alias("min_bpm"),
        F.max("duration_sec").alias("max_duration_sec_check"),
    )
    .filter(F.col("max_duration_sec_check") > 184)
    .drop("max_duration_sec_check")
)

result = (
    rollup_base
    .select("artist_id", "pct_upper_trim_artist_name_like", "avg_bpm")
    .unpivot(
        ids=["artist_id"],
        values=["pct_upper_trim_artist_name_like", "avg_bpm"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
