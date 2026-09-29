from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_to_date_r = Window.partitionBy(F.to_date("release_date_month"), "row_count").orderBy("distinct_ids")
w_all_by_row_count_moving_6 = Window.orderBy("row_count", "release_date_month").rowsBetween(-6, Window.currentRow)
w_release_date_month_month_row = Window.partitionBy(F.date_trunc("month", "release_date_month"), "row_count")
w_release_date_month_to_date_r_2 = Window.partitionBy(F.to_date("release_date_month"), "row_count").orderBy("distinct_ids", "release_date_month")
w_row_count_distinct_ids_by_di = Window.partitionBy("row_count", "distinct_ids").orderBy(F.col("distinct_ids").desc_nulls_first())

valid_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

per_group = (
    valid_tracks
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("bpm_bucket").alias("max_bpm_bucket_check"),
    )
    .filter(F.col("max_bpm_bucket_check") > 108)
    .drop("max_bpm_bucket_check")
)

with_windows = (
    per_group
    .select(
        "*",
        F.rank().over(w_release_date_month_to_date_r).alias("distinct_ids_rank"),
        F.max("distinct_ids").over(w_all_by_row_count_moving_6).alias("distinct_ids_max_7"),
        F.count(F.lit(1)).over(w_release_date_month_month_row).alias("rows_in_release_date_month_month_row_count"),
        F.lead("release_date_month", 2).over(w_release_date_month_to_date_r_2).alias("next2_release_date_month"),
        F.lag("release_date_month", 2).over(w_release_date_month_to_date_r_2).alias("prev2_release_date_month"),
        F.round(F.percent_rank().over(w_row_count_distinct_ids_by_di), 3).alias("distinct_ids_percent_rank"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("distinct_ids_max_7").alias("overall_avg_distinct_ids_max_7")))
    .filter(F.col("distinct_ids_max_7") > F.col("overall_avg_distinct_ids_max_7"))
    .drop("overall_avg_distinct_ids_max_7")
)

result = (
    with_activity
    .select(
        "release_date_month",
        "row_count",
        "distinct_ids_rank",
        "rows_in_release_date_month_month_row_count",
    )
    .unpivot(
        ids=["release_date_month"],
        values=["row_count", "distinct_ids_rank", "rows_in_release_date_month_month_row_count"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
