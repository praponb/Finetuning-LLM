from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at_2 = Window.orderBy(F.col("played_at").desc(), "id")
w_device = Window.partitionBy("device")
w_source_by_ms_played = Window.partitionBy("source").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_all_by_ms_played_rank = Window.orderBy("ms_played_rank", "id")
w_device_track_id_by_prev_ms_p = Window.partitionBy("device", "track_id").orderBy(F.col("prev_ms_played").desc(), "id")
w_ms_played_rank_next_played_a = Window.partitionBy("ms_played_rank", F.month("next_played_at"))
w_device_track_id_by_prev_ms_p_2 = Window.partitionBy("device", "track_id").orderBy(F.col("prev_ms_played").desc(), "id").rowsBetween(-1, -1)

filtered = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.lag("ms_played_filled").over(w_all_by_played_at).alias("prev_ms_played_filled"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.rank().over(w_all_by_played_at_2).alias("played_at_rank"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.lead("played_at").over(w_all_by_played_at).alias("next_played_at"),
        F.lag("ms_played", 1, 0).over(w_all_by_played_at).alias("prev_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.rank().over(w_source_by_ms_played).alias("ms_played_rank"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.least(F.col("rows_in_device"), F.col("ms_played_rank")).alias("least_rows_in_device_ms_played_rank"),
        F.least(F.col("rows_in_device"), F.col("played_at_rank")).alias("least_rows_in_device_played_at_rank"),
        F.quarter(F.col("played_at")).alias("played_at_quarter"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "prev_ms_played_filled",
        "played_at_cume_dist",
        "played_at_rank",
        "rows_in_device",
        "ms_played_vs_device_avg",
        "ms_played_rank",
        "least_rows_in_device_ms_played_rank",
        "least_rows_in_device_played_at_rank",
        "played_at_quarter",
        F.ntile(3).over(w_all_by_ms_played_rank).alias("ms_played_rank_tercile"),
        F.ntile(4).over(w_device_track_id_by_prev_ms_p).alias("prev_ms_played_quartile"),
        F.round(F.col("played_at_cume_dist") - F.avg("played_at_cume_dist").over(w_ms_played_rank_next_played_a), 2).alias("played_at_cume_dist_vs_ms_played_rank_next_playe"),
        F.sum("played_at_quarter").over(w_device_track_id_by_prev_ms_p_2).alias("played_at_quarter_sum_1_prior"),
        F.lead("next_played_at").over(w_all_by_ms_played_rank).alias("next_next_played_at"),
        F.round(F.avg("prev_ms_played_filled").over(w_ms_played_rank_next_played_a), 2).alias("ms_played_rank_next_played_at_month_avg_prev_ms_"),
        F.row_number().over(w_all_by_ms_played_rank).alias("rn"),
    )
)

totals = (
    sequenced
    .groupBy("device", "played_at_rank")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.max("played_at_cume_dist_vs_ms_played_rank_next_playe").alias("max_played_at_cume_dist_vs_ms_played_rank_next_p"),
        F.percentile_approx("ms_played_rank", 0.5).alias("median_ms_played_rank"),
    )
)

result = (
    totals
    .select(
        "device",
        "played_at_rank",
        "device_list",
        "max_played_at_cume_dist_vs_ms_played_rank_next_p",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
    .orderBy(F.col("played_at_rank").desc(), F.col("device").asc())
)
