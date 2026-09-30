from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").asc_nulls_last(), "id")
w_artist_id = Window.partitionBy("artist_id")

scoped = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.lead("duration_sec").over(w_is_explicit_by_duration_sec).alias("next_duration_sec"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_release_date)).alias("days_since_prev_release_date"),
        F.round(F.avg("bpm_bucket").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_bpm_bucket"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.lead("bpm_bucket").over(w_all_by_duration_sec).alias("next_bpm_bucket"),
        F.ntile(5).over(w_is_explicit_by_duration_sec).alias("duration_sec_quintile"),
        F.round(F.percent_rank().over(w_all_by_release_date), 3).alias("release_date_percent_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm_bucket",
        "release_date_label",
        "days_since_prev_release_date",
        "artist_id_is_explicit_avg_bpm_bucket",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "next_bpm_bucket",
        "duration_sec_quintile",
        "release_date_percent_rank",
        "rows_in_artist_id",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.when(F.col("rows_in_artist_id") >= 14, "HIGH").when(F.col("rows_in_artist_id") >= 6, "MEDIUM").otherwise("LOW").alias("rows_in_artist_id_tier"),
    )
    .orderBy(F.col("release_date").desc_nulls_first(), "rows_in_artist_id")
)
