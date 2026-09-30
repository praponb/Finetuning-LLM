from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 100)
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
    )
)

grouped = (
    cleaned_tracks
    .filter(F.col("bpm") >= 100)
    .groupBy("is_explicit")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.percentile_approx("is_explicit_flag", 0.5).alias("median_is_explicit_flag"),
        F.max("is_explicit_flag").alias("max_is_explicit_flag"),
        F.max("release_date_month_end").alias("last_release_date_month_end"),
        F.round(F.avg("is_explicit_flag"), 2).alias("avg_is_explicit_flag"),
        F.avg("is_explicit_flag").alias("avg_is_explicit_flag_check"),
    )
    .filter(F.col("avg_is_explicit_flag_check") > 0)
    .drop("avg_is_explicit_flag_check")
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.max("max_is_explicit_flag").alias("overall_max_max_is_explicit_flag")))
    .filter(F.col("max_is_explicit_flag") < F.col("overall_max_max_is_explicit_flag"))
    .drop("overall_max_max_is_explicit_flag")
)

scored = (
    filtered
    .filter(F.col("max_is_explicit_flag") >= 0)
    .select(
        "is_explicit",
        "max_is_explicit_flag",
        "last_release_date_month_end",
        "avg_is_explicit_flag",
        F.round(F.col("avg_is_explicit_flag") * 1.5, 2).alias("avg_is_explicit_flag_adj"),
    )
)

result = (
    scored
    .filter(F.col("avg_is_explicit_flag_adj") <= 1.1)
    .select(
        "is_explicit",
        "max_is_explicit_flag",
        "last_release_date_month_end",
        "avg_is_explicit_flag_adj",
    )
)
