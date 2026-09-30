from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_mo = Window.partitionBy("move_type", "warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_sku_weight_kg_full = Window.orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_sku_weight_kg = Window.orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id")
w_all_by_days_since_moved_at = Window.orderBy("days_since_moved_at", "id")
w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_category_by_sku_weight_k = Window.partitionBy("sku_category").orderBy(F.col("sku_weight_kg").asc_nulls_last())
w_sku_category_warehouse_id_by = Window.partitionBy("sku_category", "warehouse_id").orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_moving_6 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-6, Window.currentRow)
w_sku_category_by_sku_weight_k_2 = Window.partitionBy("sku_category").orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_operator_by_qty_share_of_war = Window.partitionBy("operator").orderBy(F.col("qty_share_of_warehouse_id").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_operator_by_moved_at_moving = Window.partitionBy("operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_operator_by_qty_share_of_war_2 = Window.partitionBy("operator").orderBy(F.col("qty_share_of_warehouse_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

joined_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") == "Toys"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.days_since_moved_at",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

windowed = (
    joined_sku
    .select(
        "*",
        F.max("days_since_moved_at").over(w_move_type_warehouse_id_by_mo).alias("max_days_since_moved_at_so_far"),
        F.nth_value("qty", 2).over(w_all_by_sku_weight_kg_full).alias("second_qty"),
        F.lag("sku_weight_kg", 1, 0).over(w_all_by_sku_weight_kg).alias("prev_sku_weight_kg"),
        F.row_number().over(w_all_by_days_since_moved_at).alias("days_since_moved_at_seq"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_sku_id), F.lit(0)), 4).alias("qty_share_of_operator_sku_id"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.round(F.cume_dist().over(w_sku_category_by_sku_weight_k), 3).alias("sku_weight_kg_cume_dist"),
        F.last("moved_at").over(w_sku_category_warehouse_id_by).alias("last_moved_at"),
        F.round(F.avg("qty").over(w_all_by_qty_moving_6), 2).alias("qty_avg_7"),
        F.round(F.avg("qty").over(w_sku_category_by_sku_weight_k_2), 2).alias("qty_avg_6_prior"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "days_since_moved_at",
        "sku_category",
        "second_qty",
        "prev_sku_weight_kg",
        "days_since_moved_at_seq",
        "qty_share_of_operator_sku_id",
        "sku_weight_kg_cume_dist",
        "last_moved_at",
        "qty_avg_7",
        "qty_avg_6_prior",
        F.sum("qty_avg_7").over(w_operator_by_qty_share_of_war).alias("qty_avg_7_sum_2"),
        F.sum("sku_weight_kg").over(w_operator_by_moved_at_moving).alias("sku_weight_kg_sum_7"),
        F.nth_value("qty_share_of_warehouse_id", 2).over(w_operator_by_qty_share_of_war_2).alias("second_qty_share_of_warehouse_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "days_since_moved_at",
        "sku_category",
        "second_qty",
        "days_since_moved_at_seq",
        "last_moved_at",
        "qty_avg_7",
        "qty_avg_7_sum_2",
        "sku_weight_kg_sum_7",
        "second_qty_share_of_warehouse_id",
    )
    .orderBy(F.col("second_qty").desc(), "id")
    .limit(5)
)
