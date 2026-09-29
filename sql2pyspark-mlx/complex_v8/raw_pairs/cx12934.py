from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_operator = Window.partitionBy("operator")
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at")
w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_moved_at_ties = Window.partitionBy("move_type").orderBy("moved_at")
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_minutes_since_prev_mo = Window.orderBy("minutes_since_prev_moved_at", "id")
w_distinct_sku_id_in_operator = Window.partitionBy("distinct_sku_id_in_operator")
w_all_by_minutes_since_prev_mo_2 = Window.orderBy("minutes_since_prev_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_sku_id_in_operator_2 = Window.partitionBy("distinct_sku_id_in_operator", "warehouse_id").orderBy(F.col("min_qty_so_far").asc_nulls_last(), "id")
w_distinct_sku_id_in_operator_3 = Window.partitionBy("distinct_sku_id_in_operator", "warehouse_id").orderBy(F.col("min_qty_so_far").asc_nulls_last(), "id").rowsBetween(-2, -1)

prepared_moves = (
    df_stock_moves
    .filter((F.col("operator") != "op_ann") & (F.col("sku_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

with_windows = (
    prepared_moves
    .select(
        "*",
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_by_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.min("qty").over(w_operator).alias("operator_min_qty"),
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_moved), 3).alias("moved_at_percent_rank"),
        F.first("moved_at", ignorenulls=True).over(w_move_type_by_moved_at_runnin).alias("first_known_moved_at"),
        F.lag("qty", 1, 0).over(w_move_type_by_moved_at).alias("prev_qty"),
        F.min("qty").over(w_move_type_by_moved_at_runnin).alias("min_qty_so_far"),
        F.dense_rank().over(w_move_type_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.sum("qty").over(w_move_type_by_moved_at_runnin).alias("running_qty"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.row_number().over(w_sku_id_warehouse_id_by_moved_2).alias("seq_no"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lead("moved_at_dense_rank", 3).over(w_all_by_minutes_since_prev_mo).alias("next3_moved_at_dense_rank"),
        F.round(F.col("moved_at_dense_rank_2") / F.nullif(F.sum("moved_at_dense_rank_2").over(w_distinct_sku_id_in_operator), F.lit(0)), 4).alias("moved_at_dense_rank_2_share_of_distinct_sku_id_i"),
        F.first("prev_qty", ignorenulls=True).over(w_all_by_minutes_since_prev_mo_2).alias("first_known_prev_qty"),
        F.row_number().over(w_all_by_minutes_since_prev_mo).alias("minutes_since_prev_moved_at_seq"),
        (F.col("prev_qty") - F.lag("prev_qty").over(w_distinct_sku_id_in_operator_2)).alias("prev_qty_change"),
        F.sum("moved_at_dense_rank_2").over(w_distinct_sku_id_in_operator_3).alias("moved_at_dense_rank_2_sum_2_prior"),
        F.round(F.avg("running_qty").over(w_distinct_sku_id_in_operator), 2).alias("distinct_sku_id_in_operator_avg_running_qty"),
        F.round(F.avg("min_qty_so_far").over(w_distinct_sku_id_in_operator), 2).alias("distinct_sku_id_in_operator_avg_min_qty_so_far"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "minutes_since_prev_moved_at",
        "operator_min_qty",
        "min_qty_so_far",
        "moved_at_dense_rank",
        "rows_in_operator",
        "seq_no",
        "qty_vs_operator_avg",
        "moved_at_dense_rank_2",
        "next3_moved_at_dense_rank",
        "moved_at_dense_rank_2_share_of_distinct_sku_id_i",
        "first_known_prev_qty",
        "minutes_since_prev_moved_at_seq",
        "moved_at_dense_rank_2_sum_2_prior",
        "distinct_sku_id_in_operator_avg_min_qty_so_far",
    )
    .orderBy(
        F.col("moved_at_dense_rank_2_share_of_distinct_sku_id_i").desc(),
        "rows_in_operator",
    )
)
