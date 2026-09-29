from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_track_ids_by_median = Window.partitionBy("distinct_track_ids").orderBy(F.col("median_ms_played").desc(), "device", "played_at_day", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_track_ids_by_median_2 = Window.partitionBy("distinct_track_ids").orderBy(F.col("median_ms_played").desc(), "device", "played_at_day", "is_skipped")
w_played_at_day_month = Window.partitionBy(F.month("played_at_day"))
w_all_by_min_ms_played_running = Window.orderBy("min_ms_played", "device", "played_at_day", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_track_ids_ti = Window.orderBy(F.col("distinct_track_ids").desc())
w_all_by_distinct_track_ids = Window.orderBy(F.col("distinct_track_ids").desc(), "device", "played_at_day", "is_skipped")
w_distinct_track_ids_by_median_3 = Window.partitionBy("distinct_track_ids").orderBy(F.col("median_ms_played").desc())
w_distinct_track_ids_by_median_4 = Window.partitionBy("distinct_track_ids").orderBy(F.col("median_ms_played").desc(), "device", "played_at_day", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_track_ids = Window.partitionBy("distinct_track_ids")

prepared_plays = (
    df_plays
    .filter(F.col("ms_played").between(112786, 236512))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

totals = (
    prepared_plays
    .groupBy("device", F.to_date("played_at").alias("played_at_day"), "is_skipped")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.min("ms_played").alias("min_ms_played"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
)

qualified = (
    totals
    .crossJoin(totals.agg(F.min("median_ms_played").alias("overall_min_median_ms_played")))
    .filter(F.col("median_ms_played") > F.col("overall_min_median_ms_played"))
    .drop("overall_min_median_ms_played")
)

tagged = (
    qualified
    .filter(F.col("is_skipped"))
    .select(
        "device",
        "played_at_day",
        "is_skipped",
        "last_played_at",
        "median_ms_played",
        "min_ms_played",
        "distinct_track_ids",
        F.date_format(F.col("last_played_at"), "yyyy-MM-dd").alias("last_played_at_label"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.last("distinct_track_ids", ignorenulls=True).over(w_distinct_track_ids_by_median).alias("last_known_distinct_track_ids"),
        F.row_number().over(w_distinct_track_ids_by_median_2).alias("median_ms_played_seq"),
        F.max("min_ms_played").over(w_played_at_day_month).alias("played_at_day_month_max_min_ms_played"),
        F.first("median_ms_played", ignorenulls=True).over(w_all_by_min_ms_played_running).alias("first_known_median_ms_played"),
        F.last("min_ms_played", ignorenulls=True).over(w_distinct_track_ids_by_median).alias("last_known_min_ms_played"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("median_ms_played_seq") == 1)
    .drop("median_ms_played_seq")
)

ranked = (
    top_rows
    .select(
        "*",
        F.dense_rank().over(w_all_by_distinct_track_ids_ti).alias("distinct_track_ids_dense_rank"),
        F.lead("played_at_day", 3).over(w_all_by_distinct_track_ids).alias("next3_played_at_day"),
        F.dense_rank().over(w_distinct_track_ids_by_median_3).alias("median_ms_played_dense_rank"),
        F.nth_value("distinct_track_ids", 2).over(w_distinct_track_ids_by_median_4).alias("second_distinct_track_ids"),
        F.min("min_ms_played").over(w_distinct_track_ids_by_median).alias("min_min_ms_played_so_far"),
        F.sum("last_known_min_ms_played").over(w_distinct_track_ids).alias("distinct_track_ids_sum_last_known_min_ms_played"),
    )
)

result = (
    ranked
    .filter(F.col("next3_played_at_day") < F.lit("2025-06-01").cast("date"))
    .select(
        "device",
        "played_at_day",
        "is_skipped",
        "last_played_at",
        "median_ms_played",
        "distinct_track_ids",
        "last_played_at_label",
        "last_known_distinct_track_ids",
        "played_at_day_month_max_min_ms_played",
        "first_known_median_ms_played",
        "last_known_min_ms_played",
        "distinct_track_ids_dense_rank",
        "next3_played_at_day",
        "median_ms_played_dense_rank",
        "second_distinct_track_ids",
        "min_min_ms_played_so_far",
        "distinct_track_ids_sum_last_known_min_ms_played",
    )
    .orderBy(
        F.col("last_known_min_ms_played").desc_nulls_first(),
        F.col("last_played_at").desc(),
    )
)
