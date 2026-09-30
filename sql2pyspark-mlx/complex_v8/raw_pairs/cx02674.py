from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_end_month = Window.partitionBy(F.month("release_date_month_end"), "title_prefix_len")
w_all_by_release_date_month_en = Window.orderBy(F.col("release_date_month_end_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_prefix_len_by_last_kno = Window.partitionBy("title_prefix_len").orderBy("last_known_days_since_release_date", "id")
w_title_prefix_len_release_dat = Window.partitionBy("title_prefix_len", F.year("release_date_month_end_quarter"))

recent_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

tagged = (
    recent_tracks
    .select(
        "id",
        "days_since_release_date",
        "least_bpm_duration_sec",
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
        F.coalesce(F.col("least_bpm_duration_sec"), F.lit(0)).alias("least_bpm_duration_sec_filled"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

labeled = (
    tagged
    .select(
        "*",
        F.length(F.trim(F.col("title_prefix"))).alias("title_prefix_len"),
        F.date_trunc("quarter", F.col("release_date_month_end")).alias("release_date_month_end_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("title_prefix")), " ", "_")).alias("title_prefix_slug"),
    )
)

scored = (
    labeled
    .filter(F.col("release_date_month_end") >= F.lit("2024-11-01").cast("date"))
    .withColumn("least_bpm_duration_sec_bucket", F.floor(F.col("least_bpm_duration_sec") / 50) * 50)
)

windowed = (
    scored
    .select(
        "id",
        "release_date_month_end",
        "title_prefix",
        "title_prefix_len",
        "release_date_month_end_quarter",
        "title_prefix_slug",
        "least_bpm_duration_sec_bucket",
        F.round(F.avg("title_prefix_len").over(w_release_date_month_end_month), 2).alias("release_date_month_end_month_title_prefix_len_av"),
        F.count(F.lit(1)).over(w_release_date_month_end_month).alias("rows_in_release_date_month_end_month_title_prefi"),
        F.last("days_since_release_date", ignorenulls=True).over(w_all_by_release_date_month_en).alias("last_known_days_since_release_date"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.avg("rows_in_release_date_month_end_month_title_prefi").alias("overall_avg_rows_in_release_date_month_end_month")))
    .filter(F.col("rows_in_release_date_month_end_month_title_prefi") > F.col("overall_avg_rows_in_release_date_month_end_month"))
    .drop("overall_avg_rows_in_release_date_month_end_month")
)

metrics = (
    filtered
    .withColumn("last_known_days_since_release_date_dense_rank", F.dense_rank().over(w_title_prefix_len_by_last_kno))
    .withColumn("title_prefix_len_share_of_title_prefix_len_relea", F.round(F.col("title_prefix_len") / F.nullif(F.sum("title_prefix_len").over(w_title_prefix_len_release_dat), F.lit(0)), 4))
)

result = (
    metrics
    .select(
        "id",
        "release_date_month_end",
        "title_prefix_len",
        "release_date_month_end_quarter",
        "least_bpm_duration_sec_bucket",
        "last_known_days_since_release_date",
        "last_known_days_since_release_date_dense_rank",
        (F.floor(F.col("title_prefix_len") / 2) * 2).alias("title_prefix_len_bucket"),
    )
    .orderBy(F.col("release_date_month_end").desc())
)
