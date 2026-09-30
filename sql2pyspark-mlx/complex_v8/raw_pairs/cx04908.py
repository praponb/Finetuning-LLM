from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec_per_bpm")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)

prepared = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2024-12-22").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        F.last("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_release_date"),
        F.round(F.avg("duration_sec_per_bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_duration_sec_per_bpm"),
        F.round(F.avg("bpm").over(w_is_explicit_by_release_date), 2).alias("bpm_avg_2"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.round(F.col("duration_sec_per_bpm") / F.nullif(F.sum("duration_sec_per_bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_per_bpm_share_of_artist_id_is_expli"),
        F.first("release_date").over(w_artist_id_by_release_date_fu).alias("first_release_date"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_dur), 3).alias("duration_sec_per_bpm_percent_rank"),
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_is_explicit_artist_id), 2).alias("duration_sec_per_bpm_vs_is_explicit_artist_id_av"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.ntile(4).over(w_all_by_duration_sec).alias("duration_sec_quartile"),
        F.round(F.avg("bpm").over(w_is_explicit_by_release_date_2), 2).alias("bpm_avg_1_prior"),
    )
)

tagged = (
    with_windows
    .filter(F.col("rows_in_artist_id_is_explicit") > 7)
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "last_known_release_date",
        "is_explicit_artist_id_avg_duration_sec_per_bpm",
        "rows_in_artist_id_is_explicit",
        "first_release_date",
        "duration_sec_per_bpm_percent_rank",
        "duration_sec_per_bpm_vs_is_explicit_artist_id_av",
        "rows_in_is_explicit_artist_id",
        F.abs(F.col("duration_sec_quartile") - 4).alias("duration_sec_quartile_distance"),
        F.date_add(F.col("last_known_release_date"), 90).alias("last_known_release_date_plus_90d"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

stats = (
    tagged
    .groupBy(
        F.date_format("last_known_release_date", "yyyy-MM").alias("last_known_release_date_ym"),
        "duration_sec_quartile_distance",
        "is_explicit",
    )
    .agg(
        F.round(F.avg("duration_sec_quartile_distance"), 2).alias("avg_duration_sec_quartile_distance"),
        F.round(100 * F.count(F.when(F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_is_explicit_true"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
    )
)

result = (
    stats
    .select(
        "last_known_release_date_ym",
        "duration_sec_quartile_distance",
        "is_explicit",
        "avg_duration_sec_quartile_distance",
        "pct_is_explicit_true",
        "n_is_explicit_true",
        "distinct_ids",
        "stddev_bpm",
        F.round(F.col("distinct_ids") / F.nullif(F.col("pct_is_explicit_true"), F.lit(0)), 3).alias("distinct_ids_per_pct_is_explicit_true"),
    )
    .orderBy(
        "duration_sec_quartile_distance",
        F.col("distinct_ids_per_pct_is_explicit_true").desc_nulls_first(),
    )
)
