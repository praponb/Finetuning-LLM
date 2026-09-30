from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_operator_moved_at_month_by_m = Window.partitionBy("operator", F.month("moved_at")).orderBy("moved_at", "id")
w_move_type_by_moved_at_ties = Window.partitionBy("move_type").orderBy("moved_at")
w_move_type_by_moved_at_full = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_moved_at_month_by_m_2 = Window.partitionBy("operator", F.month("moved_at")).orderBy("moved_at")
w_operator_moved_at_month_by_m_3 = Window.partitionBy("operator", F.month("moved_at")).orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_quartile_pri = Window.orderBy(F.col("moved_at_quartile").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_moved_at_to_date_moved_at_qu = Window.partitionBy(F.to_date("moved_at"), "moved_at_quartile").orderBy("moved_at")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_moved_at_dense_rank = Window.orderBy("moved_at_dense_rank", "id")
w_next_moved_at_to_date_operat = Window.partitionBy(F.to_date("next_moved_at"), "operator")
w_all_by_moved_at_quartile_ful = Window.orderBy(F.col("moved_at_quartile").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) | (F.col("moved_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

with_windows = (
    filtered_moves
    .select(
        "*",
        F.ntile(4).over(w_move_type_by_moved_at).alias("moved_at_quartile"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_moved_at_month_by_m))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.percent_rank().over(w_operator_moved_at_month_by_m), 3).alias("moved_at_percent_rank"),
        F.dense_rank().over(w_move_type_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.last("moved_at").over(w_move_type_by_moved_at_full).alias("last_moved_at"),
        F.round(F.percent_rank().over(w_operator_moved_at_month_by_m_2), 3).alias("moved_at_percent_rank_2"),
        F.lead("moved_at").over(w_operator_moved_at_month_by_m).alias("next_moved_at"),
        F.nth_value("moved_at", 2).over(w_operator_moved_at_month_by_m_3).alias("second_moved_at"),
        F.first("moved_at", ignorenulls=True).over(w_move_type_by_moved_at_runnin).alias("first_known_moved_at"),
    )
)

leaders = (
    with_windows
    .filter((F.col("moved_at_dense_rank") <= 2) & (F.col("last_moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")))
)

metrics = (
    leaders
    .select(
        "*",
        F.round(F.avg("moved_at_percent_rank").over(w_all_by_moved_at_quartile_pri), 2).alias("moved_at_percent_rank_avg_2_prior"),
        F.dense_rank().over(w_moved_at_to_date_moved_at_qu).alias("moved_at_dense_rank_2"),
        F.round(F.col("moved_at_quartile") / F.nullif(F.sum("moved_at_quartile").over(w_sku_id), F.lit(0)), 4).alias("moved_at_quartile_share_of_sku_id"),
        F.lead("moved_at_dense_rank").over(w_all_by_moved_at_dense_rank).alias("next_moved_at_dense_rank"),
        F.count(F.lit(1)).over(w_next_moved_at_to_date_operat).alias("rows_in_next_moved_at_to_date_operator"),
        F.sum("moved_at_percent_rank").over(w_next_moved_at_to_date_operat).alias("next_moved_at_to_date_operator_sum_moved_at_perc"),
        F.first("moved_at_percent_rank").over(w_all_by_moved_at_quartile_ful).alias("first_moved_at_percent_rank"),
        F.min("moved_at_dense_rank").over(w_sku_id).alias("sku_id_min_moved_at_dense_rank"),
    )
)

result = (
    metrics
    .filter(F.col("sku_id_min_moved_at_dense_rank") <= 70)
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "move_type_operator_label",
        "minutes_since_prev_moved_at",
        "moved_at_percent_rank",
        "moved_at_dense_rank",
        "moved_at_percent_rank_2",
        "second_moved_at",
        "moved_at_percent_rank_avg_2_prior",
        "moved_at_dense_rank_2",
        "next_moved_at_dense_rank",
        "rows_in_next_moved_at_to_date_operator",
        "next_moved_at_to_date_operator_sum_moved_at_perc",
        "first_moved_at_percent_rank",
        "sku_id_min_moved_at_dense_rank",
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
        F.abs(F.col("next_moved_at_dense_rank") - 6).alias("next_moved_at_dense_rank_distance"),
    )
)
