from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id")
w_artist_id_by_days_since_rele = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(-3, -1)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_days_since_rele_2 = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_id_2 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date_pr = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc())
w_moods_count_by_days_since_re = Window.partitionBy("moods_count").orderBy(F.col("days_since_release_date_2_vs_release_date_month_").desc(), "id")
w_artist_id_moods_count = Window.partitionBy("artist_id", "moods_count")
w_moods_count_by_days_since_re_2 = Window.partitionBy("moods_count").orderBy(F.col("days_since_release_date_2_vs_release_date_month_").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_max_3_pr = Window.orderBy(F.col("duration_sec_max_3_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date_2"),
    )
)

with_trends = (
    base
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "days_since_release_date",
        "days_since_release_date_2",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_month_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_release_date_month_artist_"),
        F.max("duration_sec").over(w_artist_id_by_days_since_rele).alias("duration_sec_max_3_prior"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_bpm)).alias("duration_sec_change"),
        F.round(F.percent_rank().over(w_artist_id_by_days_since_rele_2), 3).alias("days_since_release_date_percent_rank"),
        F.round(F.col("days_since_release_date_2") - F.avg("days_since_release_date_2").over(w_release_date_month), 2).alias("days_since_release_date_2_vs_release_date_month_"),
        F.round(F.avg("days_since_release_date_2").over(w_release_date_month_artist_id), 2).alias("release_date_month_artist_id_avg_days_since_rele"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_release_date_month), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_release_date_mon"),
        F.first("days_since_release_date_2", ignorenulls=True).over(w_artist_id_by_duration_sec_ru).alias("first_known_days_since_release_date_2"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_release_date_month_artist_id_2)).alias("days_since_prev_release_date"),
        F.nth_value("bpm", 2).over(w_artist_id_by_duration_sec_fu).alias("second_bpm"),
        F.max("days_since_release_date").over(w_artist_id_by_release_date_pr).alias("days_since_release_date_max_1_prior"),
        F.sum("duration_sec").over(w_artist_id_by_duration_sec_ru).alias("running_duration_sec"),
        F.round(F.percent_rank().over(w_artist_id_by_release_date_ti), 3).alias("release_date_percent_rank"),
        F.count(F.lit(1)).over(w_artist_id_release_date_month).alias("rows_in_artist_id_release_date_month"),
    )
)

annotated = (
    with_trends
    .filter(F.col("release_date_percent_rank") <= 0.7)
    .select(
        "*",
        F.size(F.col("moods")).alias("moods_count"),
        F.date_add(F.col("release_date"), 90).alias("release_date_plus_90d"),
    )
)

with_trends_2 = (
    annotated
    .select(
        "*",
        F.ntile(3).over(w_moods_count_by_days_since_re).alias("days_since_release_date_2_vs_release_date_month__2"),
        F.count(F.lit(1)).over(w_artist_id_moods_count).alias("rows_in_artist_id_moods_count"),
        F.first("bpm", ignorenulls=True).over(w_moods_count_by_days_since_re_2).alias("first_known_bpm"),
        F.last("days_since_prev_release_date").over(w_all_by_duration_sec_max_3_pr).alias("last_days_since_prev_release_date"),
    )
)

result = (
    with_trends_2
    .select("id", "days_since_release_date_max_1_prior", "artist_id")
    .distinct()
    .orderBy("id")
)
