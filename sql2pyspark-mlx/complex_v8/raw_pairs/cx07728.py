from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_sku_id_by_qty_prior_6 = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(-6, -1)
w_sku_id_by_qty_full = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_unit_price_moving = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy("qty", "id")

recent_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

windowed = (
    recent_moves
    .select(
        "id",
        "move_type",
        "moved_at",
        "unit_price",
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.lead("unit_price").over(w_sku_id_by_qty).alias("next_unit_price"),
        F.sum("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_sum_unit_price"),
        F.sum("unit_price").over(w_sku_id_by_qty_prior_6).alias("unit_price_sum_6_prior"),
        F.row_number().over(w_sku_id_by_qty).alias("rn"),
        F.min("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_min_unit_price"),
        F.round(F.cume_dist().over(w_sku_id_by_qty), 3).alias("qty_cume_dist"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_qty_full).alias("second_moved_at"),
        F.dense_rank().over(w_sku_id_by_qty).alias("qty_dense_rank"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_operator)).alias("distinct_sku_id_in_warehouse_id_operator"),
        F.max("unit_price").over(w_sku_id_by_unit_price_moving).alias("unit_price_max_7"),
        F.round(F.cume_dist().over(w_move_type_by_qty), 3).alias("qty_cume_dist_2"),
        F.size(F.collect_set("move_type").over(w_operator_warehouse_id)).alias("distinct_move_type_in_operator_warehouse_id"),
    )
)

top_per_group = windowed.filter(F.col("qty_dense_rank") == 1)

result = (
    top_per_group
    .filter(F.col("qty_cume_dist_2") > 0.3)
    .select(
        "id",
        "move_type",
        "moved_at",
        "unit_price",
        "next_unit_price",
        "operator_warehouse_id_sum_unit_price",
        "unit_price_sum_6_prior",
        "qty_cume_dist",
        "second_moved_at",
        "qty_dense_rank",
        "unit_price_max_7",
        "qty_cume_dist_2",
        "distinct_move_type_in_operator_warehouse_id",
        F.when(F.col("unit_price") >= 329.7, "A").when(F.col("unit_price") >= 152.7, "B").otherwise("C").alias("unit_price_tier"),
    )
)
