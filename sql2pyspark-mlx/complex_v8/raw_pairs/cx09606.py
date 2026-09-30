from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_bpm_tier_by_releas = Window.partitionBy("artist_id", "bpm_tier").orderBy(F.col("release_date").desc_nulls_first())
w_release_date_year_bpm_tier_b = Window.partitionBy(F.year("release_date"), "bpm_tier").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_bpm_tier_by_releas_2 = Window.partitionBy("artist_id", "bpm_tier").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year_bpm_tier_b_2 = Window.partitionBy(F.year("release_date"), "bpm_tier").orderBy("bpm")
w_bpm_tier = Window.partitionBy("bpm_tier")
w_dedup = Window.partitionBy("artist_id").orderBy("release_date", "id")

recent_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.when(F.col("bpm") >= 143, "large").when(F.col("bpm") >= 85, "medium").otherwise("small").alias("bpm_tier"),
    )
)

with_windows = (
    recent_tracks
    .select(
        "*",
        F.dense_rank().over(w_artist_id_bpm_tier_by_releas).alias("release_date_dense_rank"),
        F.nth_value("bpm", 2).over(w_release_date_year_bpm_tier_b).alias("second_bpm"),
        F.last("duration_sec").over(w_artist_id_bpm_tier_by_releas_2).alias("last_duration_sec"),
        F.round(F.percent_rank().over(w_release_date_year_bpm_tier_b_2), 3).alias("bpm_percent_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_bpm_tier), F.lit(0)), 4).alias("duration_sec_share_of_bpm_tier"),
        F.max("bpm").over(w_bpm_tier).alias("bpm_tier_max_bpm"),
    )
)

latest = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "bpm_tier_max_bpm",
        "second_bpm",
        "last_duration_sec",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["bpm_tier_max_bpm", "second_bpm", "last_duration_sec", "duration_sec"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("id", "field")
)
