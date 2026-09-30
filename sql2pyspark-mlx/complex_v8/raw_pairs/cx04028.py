from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_moved_at_moving_3 = Window.orderBy("moved_at", "id").rowsBetween(-3, Window.currentRow)
w_move_type_warehouse_id_by_qt = Window.partitionBy("move_type", "warehouse_id").orderBy("qty")
w_all_by_moved_at_prior_1 = Window.orderBy("moved_at", "id").rowsBetween(-1, -1)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_all_by_moved_at_moving_1 = Window.orderBy("moved_at", "id").rowsBetween(-1, Window.currentRow)
w_move_type_warehouse_id_by_qt_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_warehouse_id_by_qt_3 = Window.partitionBy("move_type", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_qty = Window.orderBy("qty", "id")

valid = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_details_sku = (
    valid.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

ranked = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "sku_code",
        "sku_unit_cost",
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_warehouse_id), 2).alias("sku_unit_cost_vs_warehouse_id_avg"),
        F.round(F.avg("qty").over(w_all_by_moved_at_moving_3), 2).alias("qty_avg_4"),
        F.round(F.cume_dist().over(w_move_type_warehouse_id_by_qt), 3).alias("qty_cume_dist"),
        F.round(F.avg("qty").over(w_all_by_moved_at_prior_1), 2).alias("qty_avg_1_prior"),
        F.sum("qty").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_sum_qty"),
        F.sum("qty").over(w_all_by_moved_at_moving_1).alias("qty_sum_2"),
        F.first("qty").over(w_move_type_warehouse_id_by_qt_2).alias("first_qty"),
        F.max("qty").over(w_move_type_warehouse_id_by_qt_3).alias("max_qty_so_far"),
        F.last("moved_at", ignorenulls=True).over(w_warehouse_id_by_moved_at_run).alias("last_known_moved_at"),
        F.min("sku_unit_cost").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_min_sku_unit_cost"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.rank().over(w_all_by_qty).alias("qty_rank"),
        F.rank().over(w_move_type_warehouse_id_by_qt).alias("qty_rank_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_unit_cost_vs_warehouse_id_avg",
        "qty_avg_4",
        "qty_cume_dist",
        "qty_avg_1_prior",
        "warehouse_id_move_type_sum_qty",
        "qty_sum_2",
        "max_qty_so_far",
        "last_known_moved_at",
        "moved_at_dense_rank",
        "qty_rank",
        "qty_rank_2",
        F.lower(F.regexp_replace(F.trim(F.col("sku_code")), " ", "_")).alias("sku_code_slug"),
    )
)
