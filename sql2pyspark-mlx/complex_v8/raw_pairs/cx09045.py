from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_move_type_by = Window.partitionBy(F.date_trunc("month", "moved_at"), "move_type").orderBy("unit_price_adj", "id").rowsBetween(-6, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_prior_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, -1)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_moved_at_month_move_type_by_2 = Window.partitionBy(F.date_trunc("month", "moved_at"), "move_type").orderBy("unit_price_adj", "id")
w_all_by_unit_price_moving_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, Window.currentRow)
w_all_by_moved_at_full_2 = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_moved_at_rank_warehouse_id_b = Window.partitionBy("moved_at_rank", "warehouse_id").orderBy(F.col("moved_at_rank").desc_nulls_first(), "id")
w_sku_id_warehouse_id_by_prev = Window.partitionBy("sku_id", "warehouse_id").orderBy("prev_unit_price_2")

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

ranked = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_adj",
        F.round(F.avg("unit_price").over(w_moved_at_month_move_type_by), 2).alias("unit_price_avg_7"),
        F.sum("unit_price_adj").over(w_warehouse_id).alias("warehouse_id_sum_unit_price_adj"),
        F.lead("unit_price_adj").over(w_all_by_unit_price).alias("next_unit_price_adj"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.round(F.avg("unit_price_adj").over(w_all_by_unit_price_prior_1), 2).alias("unit_price_adj_avg_1_prior"),
        F.lag("unit_price", 1, 0).over(w_all_by_unit_price).alias("prev_unit_price"),
        F.lag("unit_price", 1, 0).over(w_all_by_moved_at).alias("prev_unit_price_2"),
        F.first("unit_price_adj").over(w_all_by_moved_at_full).alias("first_unit_price_adj"),
        F.round(F.avg("unit_price_adj").over(w_moved_at_month), 2).alias("moved_at_month_avg_unit_price_adj"),
        F.round(F.col("qty") - F.avg("qty").over(w_moved_at_month), 2).alias("qty_vs_moved_at_month_avg"),
        F.lead("unit_price_adj").over(w_moved_at_month_move_type_by_2).alias("next_unit_price_adj_2"),
        F.max("qty").over(w_all_by_unit_price_moving_1).alias("qty_max_2"),
        F.nth_value("unit_price", 2).over(w_all_by_moved_at_full).alias("second_unit_price"),
        F.nth_value("unit_price_adj", 2).over(w_all_by_moved_at_full_2).alias("second_unit_price_adj"),
        F.rank().over(w_all_by_moved_at).alias("moved_at_rank"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "unit_price_adj",
        "unit_price_avg_7",
        "unit_price_vs_warehouse_id_avg",
        "unit_price_adj_avg_1_prior",
        "prev_unit_price_2",
        "moved_at_month_avg_unit_price_adj",
        "next_unit_price_adj_2",
        "second_unit_price_adj",
        "moved_at_rank",
        F.lag("unit_price_avg_7").over(w_moved_at_rank_warehouse_id_b).alias("prev_unit_price_avg_7"),
        F.lead("prev_unit_price").over(w_moved_at_rank_warehouse_id_b).alias("next_prev_unit_price"),
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_prev), 3).alias("prev_unit_price_2_cume_dist"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.round(F.col("unit_price_adj") / F.nullif(F.sum("unit_price_adj").over(w_warehouse_id_sku_id), F.lit(0)), 4).alias("unit_price_adj_share_of_warehouse_id_sku_id"),
    )
)

result = (
    windowed
    .filter(F.col("moved_at") < F.lit("2025-02-12 15:00:00").cast("timestamp"))
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "unit_price_adj",
        "unit_price_avg_7",
        "unit_price_adj_avg_1_prior",
        "prev_unit_price_2",
        "moved_at_month_avg_unit_price_adj",
        "next_unit_price_adj_2",
        "moved_at_rank",
        "next_prev_unit_price",
        "rows_in_warehouse_id_sku_id",
        "unit_price_adj_share_of_warehouse_id_sku_id",
        F.abs(F.col("prev_unit_price_2") - 209.4).alias("prev_unit_price_2_distance"),
        (F.floor(F.col("prev_unit_price_2") / 100) * 100).alias("prev_unit_price_2_bucket"),
    )
)
