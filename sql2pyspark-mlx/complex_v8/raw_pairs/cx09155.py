from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy("qty")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_dense_rank = Window.partitionBy("moved_at_dense_rank")
w_move_type_by_move_type_wareh = Window.partitionBy("move_type").orderBy("move_type_warehouse_id_min_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_move_type_wareh_2 = Window.partitionBy("move_type").orderBy("move_type_warehouse_id_min_qty", "id")
w_sku_id_by_qty_vs_move_type_w = Window.partitionBy("sku_id").orderBy(F.col("qty_vs_move_type_warehouse_id_avg").desc(), "id")
w_move_type_by_move_type_wareh_3 = Window.partitionBy("move_type").orderBy("move_type_warehouse_id_min_qty", "id").rowsBetween(-2, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 271) & (~F.col("operator").isin("op_eve", "op_chan")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
    )
)

with_windows = (
    scoped_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_adj",
        F.rank().over(w_warehouse_id_by_qty_ties).alias("qty_rank"),
        F.max("unit_price_adj").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_unit_price_adj"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_move_type_warehouse_id"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type_warehouse_id), 2).alias("qty_vs_move_type_warehouse_id_avg"),
        F.round(F.cume_dist().over(w_warehouse_id_by_qty), 3).alias("qty_cume_dist"),
        F.lag("unit_price").over(w_all_by_moved_at).alias("prev_unit_price"),
        F.row_number().over(w_all_by_unit_price).alias("unit_price_seq"),
        F.min("qty").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_qty"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type_warehouse_id)).alias("distinct_sku_id_in_move_type_warehouse_id"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
        F.count(F.lit(1)).over(w_warehouse_id_by_qty_running).alias("running_count"),
        F.nth_value("unit_price_adj", 2).over(w_warehouse_id_by_qty_full).alias("second_unit_price_adj"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.max("qty").over(w_moved_at_dense_rank).alias("moved_at_dense_rank_max_qty"),
        F.nth_value("moved_at_dense_rank", 2).over(w_move_type_by_move_type_wareh).alias("second_moved_at_dense_rank"),
        F.size(F.collect_set("sku_id").over(w_moved_at_dense_rank)).alias("distinct_sku_id_in_moved_at_dense_rank"),
        F.row_number().over(w_move_type_by_move_type_wareh_2).alias("rn_2"),
        F.lag("unit_price", 3).over(w_sku_id_by_qty_vs_move_type_w).alias("prev3_unit_price"),
        F.sum("qty_rank").over(w_move_type_by_move_type_wareh_3).alias("qty_rank_sum_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "unit_price_adj",
        "qty_rank",
        "move_type_warehouse_id_max_unit_price_adj",
        "unit_price_share_of_move_type_warehouse_id",
        "qty_vs_move_type_warehouse_id_avg",
        "qty_cume_dist",
        "move_type_warehouse_id_min_qty",
        "moved_at_dense_rank",
        "distinct_sku_id_in_move_type_warehouse_id",
        "rn",
        "second_unit_price_adj",
        "moved_at_dense_rank_max_qty",
        "second_moved_at_dense_rank",
        "distinct_sku_id_in_moved_at_dense_rank",
        "rn_2",
        "prev3_unit_price",
        "qty_rank_sum_3",
        F.least(F.col("rn"), F.col("prev3_unit_price")).alias("least_rn_prev3_unit_price"),
    )
    .orderBy(F.col("moved_at").asc_nulls_last(), "id")
    .limit(10)
)
