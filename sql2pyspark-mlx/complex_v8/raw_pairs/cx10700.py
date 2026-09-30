from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_device_played_at_month_by_pl = Window.partitionBy("device", F.month("played_at")).orderBy(F.col("played_at").desc())
w_device_played_at_month_by_pl_2 = Window.partitionBy("device", F.month("played_at")).orderBy(F.col("played_at").desc(), "id")
w_device_played_at_month_by_pl_3 = Window.partitionBy("device", F.month("played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_played_at = Window.partitionBy("source", "track_id").orderBy("played_at", "id")
w_source_track_id_by_played_at_2 = Window.partitionBy("source", "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_played_at_3 = Window.partitionBy("source", "track_id").orderBy("played_at", "id").rowsBetween(-3, -1)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_source_by_next_played = Window.partitionBy("device", "source").orderBy("next_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_device_avg_ms_played = Window.orderBy(F.col("device_avg_ms_played").desc(), "id").rowsBetween(-6, -1)
w_all_by_device_avg_ms_played_2 = Window.orderBy(F.col("device_avg_ms_played").desc(), "id")
w_distinct_track_id_in_device = Window.partitionBy("distinct_track_id_in_device", "played_at_rank")

base_plays = (
    df_plays
    .filter((F.col("ms_played") <= 225733) | (F.col("source") != "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

metrics = (
    base_plays
    .select(
        "*",
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.lag("played_at").over(w_track_id_by_ms_played).alias("prev_played_at"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
        F.round(F.percent_rank().over(w_device_played_at_month_by_pl), 3).alias("played_at_percent_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.lead("played_at").over(w_device_played_at_month_by_pl_2).alias("next_played_at"),
        F.nth_value("played_at", 2).over(w_device_played_at_month_by_pl_3).alias("second_played_at"),
        F.lag("ms_played").over(w_source_track_id_by_played_at).alias("prev_ms_played"),
        F.rank().over(w_device_played_at_month_by_pl).alias("played_at_rank"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.first("ms_played").over(w_source_track_id_by_played_at_2).alias("first_ms_played"),
        F.lead("ms_played").over(w_device_played_at_month_by_pl_2).alias("next_ms_played"),
        F.round(F.avg("ms_played").over(w_source_track_id_by_played_at_3), 2).alias("ms_played_avg_3_prior"),
        F.last("ms_played", ignorenulls=True).over(w_track_id_by_ms_played_runnin).alias("last_known_ms_played"),
        F.lead("played_at", 3).over(w_device_played_at_month_by_pl_2).alias("next3_played_at"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("played_at_rank") <= 3) & (F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
)

ranked = (
    top_per_group
    .select(
        "*",
        F.first("ms_played", ignorenulls=True).over(w_device_source_by_next_played).alias("first_known_ms_played"),
        F.round(F.avg("prev_ms_played").over(w_all_by_device_avg_ms_played), 2).alias("prev_ms_played_avg_6_prior"),
        F.row_number().over(w_all_by_device_avg_ms_played_2).alias("device_avg_ms_played_seq"),
        F.count(F.lit(1)).over(w_distinct_track_id_in_device).alias("rows_in_distinct_track_id_in_device_played_at_ra"),
    )
)

totals = (
    ranked
    .filter(F.col("played_at_percent_rank") >= 0.4)
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        F.year("second_played_at").alias("second_played_at_year"),
    )
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.sum(F.when(F.col("device").isNotNull(), 1).otherwise(0)).alias("n_device_is_not_null"),
        F.sum("rows_in_distinct_track_id_in_device_played_at_ra").alias("total_rows_in_distinct_track_id_in_device_played"),
    )
)

result = (
    totals
    .select(
        "played_at_month",
        "second_played_at_year",
        "device_list",
        "n_device_is_not_null",
        "total_rows_in_distinct_track_id_in_device_played",
    )
    .orderBy(F.col("second_played_at_year").desc_nulls_first(), "played_at_month")
    .limit(10)
)
