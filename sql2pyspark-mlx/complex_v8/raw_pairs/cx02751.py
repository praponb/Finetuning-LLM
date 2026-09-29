from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_moving_2 = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_release_date_running = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_tracks.filter(F.col("bpm") >= 76)

with_windows = (
    scoped
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.round(F.avg("bpm").over(w_all_by_release_date_moving_2), 2).alias("bpm_avg_3"),
        F.last("bpm", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_bpm"),
    )
)

enriched = (
    with_windows
    .filter(F.col("bpm_avg_3") >= 78.0)
    .select(
        "*",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 213, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec_bucket").cast("string")).alias("title_ref"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.coalesce(F.col("duration_sec_tier"), F.lit("UNKNOWN")).alias("duration_sec_tier_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "title",
        "release_date",
        "moods",
        "last_known_bpm",
        "title_ref",
        "release_date_month",
        F.concat_ws(" / ", F.col("duration_sec_tier"), F.col("title_ref")).alias("duration_sec_tier_title_ref_label"),
    )
    .orderBy(F.col("duration_sec_tier_title_ref_label").desc(), "id")
    .limit(20)
)
