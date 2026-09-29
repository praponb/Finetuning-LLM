from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_month_fu = Window.orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_month = Window.orderBy("release_date_month", "id")
w_all_by_release_date_month_ru = Window.orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_3_is_expl = Window.partitionBy("release_date_month_3", "is_explicit")
w_all_by_release_date_month_mo = Window.orderBy("release_date_month", "id").rowsBetween(-1, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month_2"),
        F.month(F.col("release_date")).alias("release_date_month_3"),
    )
)

metrics = (
    recent_tracks
    .select(
        "*",
        F.first("duration_sec").over(w_all_by_release_date_month_fu).alias("first_duration_sec"),
        F.round((F.unix_timestamp("release_date_month_2") - F.unix_timestamp(F.lag("release_date_month_2").over(w_all_by_release_date_month))) / 60, 1).alias("minutes_since_prev_release_date_month_2"),
        F.first("bpm", ignorenulls=True).over(w_all_by_release_date_month_ru).alias("first_known_bpm"),
        F.max("release_date_month_3").over(w_release_date_month_3_is_expl).alias("release_date_month_3_is_explicit_max_release_dat"),
        F.round(F.avg("release_date_month_3").over(w_all_by_release_date_month_mo), 2).alias("release_date_month_3_avg_2"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.max("duration_sec").alias("overall_max_duration_sec")))
    .filter(F.col("duration_sec") < F.col("overall_max_duration_sec"))
    .drop("overall_max_duration_sec")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.max("first_known_bpm").alias("overall_max_first_known_bpm")))
    .filter(F.col("first_known_bpm") < F.col("overall_max_first_known_bpm"))
    .drop("overall_max_first_known_bpm")
)

result = (
    with_activity
    .filter(F.upper(F.trim(F.col("title"))).like("E%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_month",
        "release_date_month_3",
        "first_duration_sec",
        "minutes_since_prev_release_date_month_2",
        "first_known_bpm",
        "release_date_month_3_is_explicit_max_release_dat",
        "release_date_month_3_avg_2",
    )
    .orderBy("release_date", "id")
    .limit(5)
)
