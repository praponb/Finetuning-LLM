from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_has_party = Window.partitionBy("has_party")
w_has_party_by_min_duration_se = Window.partitionBy("has_party").orderBy("min_duration_sec_filled", "has_party")

recent_tracks = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.array_contains(F.col("moods"), "party").alias("has_party"),
    )
)

grouped = (
    recent_tracks
    .filter(F.col("artist_id") <= 9)
    .groupBy("has_party")
    .agg(
        F.min("duration_sec_filled").alias("min_duration_sec_filled"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.min("release_date").alias("first_release_date"),
        F.sum(F.when(F.col("is_explicit"), F.col("bpm")).otherwise(0)).alias("bpm_is_explicit_true"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

metrics = (
    grouped
    .select(
        "*",
        F.sum("bpm_is_explicit_true").over(w_has_party).alias("has_party_sum_bpm_is_explicit_true"),
        F.dense_rank().over(w_has_party_by_min_duration_se).alias("min_duration_sec_filled_dense_rank"),
        F.min("n_is_explicit_false").over(w_has_party).alias("has_party_min_n_is_explicit_false"),
        F.min("min_duration_sec_filled").over(w_has_party).alias("has_party_min_min_duration_sec_filled"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.avg("has_party_min_n_is_explicit_false").alias("overall_avg_has_party_min_n_is_explicit_false")))
    .filter(F.col("has_party_min_n_is_explicit_false") > F.col("overall_avg_has_party_min_n_is_explicit_false"))
    .drop("overall_avg_has_party_min_n_is_explicit_false")
)

labeled = (
    without_matches
    .filter(F.col("bpm_is_explicit_true") >= 180)
    .select(
        "*",
        F.when(F.col("has_party"), "yes").otherwise("no").alias("has_party_label"),
        F.when(F.col("n_rows") >= 10, "large").when(F.col("n_rows") >= 5, "medium").otherwise("small").alias("n_rows_tier"),
        F.when(F.col("has_party"), "yes").otherwise("no").alias("has_party_label_2"),
    )
)

result = (
    labeled
    .select(
        "has_party",
        "n_rows",
        "has_party_min_min_duration_sec_filled",
        "min_duration_sec_filled_dense_rank",
        "n_is_explicit_false",
    )
    .unpivot(
        ids=["has_party"],
        values=["n_rows", "has_party_min_min_duration_sec_filled", "min_duration_sec_filled_dense_rank", "n_is_explicit_false"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
