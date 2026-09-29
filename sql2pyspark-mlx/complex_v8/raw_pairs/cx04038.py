from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc())

scoped = df_tracks.filter((F.col("bpm") < 166) & (~F.col("is_explicit")))

sequenced = (
    scoped
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.nth_value("release_date", 2).over(w_all_by_bpm_full).alias("second_release_date"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("bpm_rank") <= 3)
    .drop("bpm_rank")
)

grouped = (
    first_rows
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.count(F.when(F.lower(F.col("title")).like("%bu%"), 1)).alias("n_lower_title_like_bu"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("artist_id_max_duration_sec"), 2).alias("avg_artist_id_max_duration_sec"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
    )
)

result = (
    grouped
    .filter(F.col("row_count") >= 6)
    .select(
        "release_date_month",
        "n_lower_title_like_bu",
        "distinct_ids",
        "avg_artist_id_max_duration_sec",
        "n_is_explicit_true",
        F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"),
        F.when(F.col("n_lower_title_like_bu") >= 8, "HIGH").when(F.col("n_lower_title_like_bu") >= 4, "MEDIUM").otherwise("LOW").alias("n_lower_title_like_bu_tier"),
    )
    .orderBy(F.col("n_lower_title_like_bu").desc())
)
