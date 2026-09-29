from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("duration_sec") >= 209))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.abs(F.col("duration_sec") - 209).alias("duration_sec_distance"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

labeled = (
    filtered_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "title_prefix",
        F.abs(F.col("duration_sec") - 309).alias("duration_sec_distance_2"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

with_parent_artist = (
    labeled.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "inner")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

rollup_base = (
    with_parent_artist
    .groupBy("is_explicit_label", "artist_is_verified")
    .agg(
        F.round(100 * F.count(F.when(F.col("artist_is_verified"), 1)) / F.count("*"), 1).alias("pct_artist_is_verified_true"),
        F.count("*").alias("cnt"),
        F.countDistinct("is_explicit_label").alias("distinct_is_explicit_labels"),
        F.round(F.avg("duration_sec_distance_2"), 2).alias("avg_duration_sec_distance_2"),
        F.round(F.sum("duration_sec_distance_2") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("duration_sec_distance_2_to_duration_sec_ratio"),
        F.max("duration_sec_distance_2").alias("max_duration_sec_distance_2"),
    )
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

result = (
    filtered
    .select(
        "is_explicit_label",
        "artist_is_verified",
        "max_duration_sec_distance_2",
        "cnt",
        "distinct_is_explicit_labels",
    )
    .unpivot(
        ids=["is_explicit_label", "artist_is_verified"],
        values=["max_duration_sec_distance_2", "cnt", "distinct_is_explicit_labels"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
