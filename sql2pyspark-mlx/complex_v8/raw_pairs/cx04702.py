from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_bpm_quintile = Window.partitionBy("bpm_quintile")
w_bpm_quintile_by_duration_sec = Window.partitionBy("bpm_quintile").orderBy(F.col("duration_sec_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_quintile_by_duration_sec_2 = Window.partitionBy("bpm_quintile").orderBy(F.col("duration_sec_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.upper(F.trim(F.col("title"))).like("M%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

ranked = (
    valid_tracks
    .select(
        "id",
        "title",
        "moods",
        "days_since_release_date",
        "release_date_label",
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_bpm)).alias("duration_sec_change"),
        F.lead("bpm").over(w_all_by_bpm).alias("next_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.ntile(5).over(w_all_by_bpm).alias("bpm_quintile"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "days_since_release_date",
        "duration_sec_change",
        "next_bpm",
        "duration_sec_vs_artist_id_avg",
        F.min("next_bpm").over(w_bpm_quintile).alias("bpm_quintile_min_next_bpm"),
        F.sum("bpm_quintile").over(w_bpm_quintile_by_duration_sec).alias("running_bpm_quintile"),
        F.sum("duration_sec_change").over(w_bpm_quintile_by_duration_sec).alias("running_duration_sec_change"),
        F.nth_value("duration_sec_change", 2).over(w_bpm_quintile_by_duration_sec_2).alias("second_duration_sec_change"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "days_since_release_date",
        "duration_sec_change",
        "next_bpm",
        "bpm_quintile_min_next_bpm",
        "running_bpm_quintile",
        "running_duration_sec_change",
        "second_duration_sec_change",
        (F.col("running_bpm_quintile") - F.col("days_since_release_date")).alias("running_bpm_quintile_minus_days_since_release_da"),
        F.round(F.col("duration_sec_change") / F.nullif(F.col("days_since_release_date"), F.lit(0)), 3).alias("duration_sec_change_per_days_since_release_date"),
    )
)

result = (
    tagged
    .select(
        "id",
        "days_since_release_date",
        "duration_sec_change",
        "next_bpm",
        "bpm_quintile_min_next_bpm",
        "running_bpm_quintile",
        "second_duration_sec_change",
        "running_bpm_quintile_minus_days_since_release_da",
        "duration_sec_change_per_days_since_release_date",
        F.round(F.col("days_since_release_date") / F.nullif(F.col("next_bpm"), F.lit(0)), 3).alias("days_since_release_date_per_next_bpm"),
    )
    .orderBy(F.col("days_since_release_date_per_next_bpm").desc_nulls_first())
)
