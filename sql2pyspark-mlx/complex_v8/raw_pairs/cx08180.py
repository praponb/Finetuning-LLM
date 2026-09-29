from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_bpm_tier_is_explicit_by_bpm = Window.partitionBy("bpm_tier", "is_explicit").orderBy("bpm", "id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_bpm_tier = Window.partitionBy("artist_id", "bpm_tier")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_bpm_tier = Window.partitionBy("bpm_tier")
w_bpm_tier_artist_id_by_releas = Window.partitionBy("bpm_tier", "artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_bpm_tier_by_duration_sec_run = Window.partitionBy("bpm_tier").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_tier_artist_id_by_releas_2 = Window.partitionBy("bpm_tier", "artist_id").orderBy(F.col("release_date").desc())
w_bpm_tier_by_duration_sec = Window.partitionBy("bpm_tier").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc())

scoped_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 94) & (F.col("artist_id") <= 3))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 156, "A").when(F.col("bpm") >= 94, "B").otherwise("C").alias("bpm_tier"),
    )
)

ranked = (
    scoped_tracks
    .select(
        "*",
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_bpm_tier_is_explicit_by_bpm)).alias("days_since_prev_release_date"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit_by_dur), 2).alias("bpm_avg_3_prior"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_bpm_tier), 2).alias("duration_sec_vs_artist_id_bpm_tier_avg"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.count(F.lit(1)).over(w_bpm_tier).alias("rows_in_bpm_tier"),
        F.round(F.avg("duration_sec").over(w_bpm_tier_artist_id_by_releas), 2).alias("duration_sec_avg_7"),
        F.count(F.lit(1)).over(w_artist_id_bpm_tier).alias("rows_in_artist_id_bpm_tier"),
        F.min("bpm").over(w_bpm_tier_by_duration_sec_run).alias("min_bpm_so_far"),
        F.rank().over(w_bpm_tier_artist_id_by_releas_2).alias("release_date_rank"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_bpm_tier_by_duration_sec)).alias("days_since_prev_release_date_2"),
        F.lag("release_date").over(w_bpm_tier_by_duration_sec).alias("prev_release_date"),
        F.rank().over(w_artist_id_is_explicit_by_dur_2).alias("duration_sec_rank"),
        F.min("duration_sec").over(w_artist_id_bpm_tier).alias("artist_id_bpm_tier_min_duration_sec"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_is_explicit").between(5, 18))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "bpm_avg_3_prior",
        "duration_sec_share_of_is_explicit",
        "duration_sec_vs_artist_id_bpm_tier_avg",
        "rows_in_is_explicit",
        "duration_sec_percent_rank",
        "rows_in_artist_id_bpm_tier",
        "min_bpm_so_far",
        "days_since_prev_release_date_2",
        "duration_sec_rank",
        "artist_id_bpm_tier_min_duration_sec",
        F.abs(F.col("bpm") - 143).alias("bpm_distance"),
        F.abs(F.col("rows_in_is_explicit") - 9).alias("rows_in_is_explicit_distance"),
    )
    .orderBy(F.col("duration_sec_share_of_is_explicit").desc(), "bpm", "id")
    .limit(20)
)
