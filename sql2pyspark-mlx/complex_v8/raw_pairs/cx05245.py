from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_least_bpm_duration_se = Window.orderBy("least_bpm_duration_sec")
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy("duration_sec", "id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc())
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_bpm_quintile_duration_sec_ra = Window.partitionBy("bpm_quintile", "duration_sec_rank")
w_all_by_duration_sec_percent = Window.orderBy(F.col("duration_sec_percent_rank_2").desc())

base = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("R%")) & (F.col("artist_id") <= 11))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

with_trends = (
    base
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "least_bpm_duration_sec",
        F.round(F.cume_dist().over(w_all_by_least_bpm_duration_se), 3).alias("least_bpm_duration_sec_cume_dist"),
        F.round(F.percent_rank().over(w_release_date_year_by_duratio), 3).alias("duration_sec_percent_rank"),
        F.ntile(5).over(w_is_explicit_artist_id_by_bpm).alias("bpm_quintile"),
        F.sum("least_bpm_duration_sec").over(w_is_explicit).alias("is_explicit_sum_least_bpm_duration_sec"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank_2"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.round(F.percent_rank().over(w_artist_id_is_explicit_by_rel), 3).alias("release_date_percent_rank"),
        F.lag("bpm", 3).over(w_artist_id_is_explicit_by_rel_2).alias("prev3_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_rel_2)).alias("bpm_change"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id_by_bpm_2).alias("running_count"),
        F.rank().over(w_release_date_year_by_duratio).alias("duration_sec_rank"),
        F.round(F.col("least_bpm_duration_sec") / F.nullif(F.sum("least_bpm_duration_sec").over(w_artist_id), F.lit(0)), 4).alias("least_bpm_duration_sec_share_of_artist_id"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.round(F.col("least_bpm_duration_sec") - F.avg("least_bpm_duration_sec").over(w_bpm_quintile_duration_sec_ra), 2).alias("least_bpm_duration_sec_vs_bpm_quintile_duration_"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_percent), 3).alias("duration_sec_percent_rank_2_percent_rank"),
        F.round(F.col("prev3_bpm") - F.avg("prev3_bpm").over(w_bpm_quintile_duration_sec_ra), 2).alias("prev3_bpm_vs_bpm_quintile_duration_sec_rank_avg"),
    )
)

summary = (
    metrics
    .groupBy(
        "bpm_quintile",
        "duration_sec_rank",
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
    )
    .agg(
        F.min("release_date").alias("first_release_date"),
        F.count("*").alias("n_rows"),
        F.max("release_date").alias("last_release_date"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
    )
)

result = (
    summary
    .select(
        "bpm_quintile",
        "duration_sec_rank",
        "release_date_ym",
        "first_release_date",
        "n_rows",
        "last_release_date",
        "n_is_explicit_true",
    )
    .orderBy("n_is_explicit_true")
)
