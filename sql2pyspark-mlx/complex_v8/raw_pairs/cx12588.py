from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_distance_full = Window.orderBy(F.col("bpm_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(-1, -1)
w_release_date_month_is_explic_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_distance_prior_6 = Window.orderBy(F.col("bpm_distance").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_second_bpm_distance_m = Window.orderBy("second_bpm_distance", "id").rowsBetween(-6, Window.currentRow)
w_all_by_second_release_date = Window.orderBy("second_release_date", "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_second_bpm_distance = Window.orderBy("second_bpm_distance", "id")
w_all_by_second_release_date_m = Window.orderBy("second_release_date", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_tracks
    .filter((F.col("duration_sec") >= 154) & (F.upper(F.trim(F.col("title"))).like("N%")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.abs(F.col("bpm") - 94).alias("bpm_distance"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.nth_value("release_date", 2).over(w_all_by_bpm_distance_full).alias("second_release_date"),
        F.first("bpm_distance").over(w_all_by_bpm_distance_full).alias("first_bpm_distance"),
        F.max("duration_sec").over(w_release_date_month_is_explic).alias("duration_sec_max_1_prior"),
        F.nth_value("bpm_distance", 2).over(w_release_date_month_is_explic_2).alias("second_bpm_distance"),
        F.sum("bpm_distance").over(w_all_by_bpm_distance_prior_6).alias("bpm_distance_sum_6_prior"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.sum("second_bpm_distance").over(w_all_by_second_bpm_distance_m).alias("second_bpm_distance_sum_7"),
        F.row_number().over(w_all_by_second_release_date).alias("rn"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.lead("bpm_distance", 3, 0).over(w_all_by_second_bpm_distance).alias("next3_bpm_distance"),
        F.round(F.avg("duration_sec").over(w_all_by_second_release_date_m), 2).alias("duration_sec_avg_3"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 166), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "rn",
        "rows_in_artist_id",
        "second_bpm_distance_sum_7",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["rn", "rows_in_artist_id", "second_bpm_distance_sum_7", "duration_sec"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
