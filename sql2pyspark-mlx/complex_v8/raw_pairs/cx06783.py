from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_is_active_by_qty_filled = Window.partitionBy("sku_is_active").orderBy(F.col("qty_filled").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_sku_is_active_by_qty_filled_2 = Window.partitionBy("sku_is_active").orderBy(F.col("qty_filled").asc_nulls_last(), "id")
w_all_by_moved_at_prior_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_sku_is_active_by_qty = Window.partitionBy("sku_id", "sku_is_active").orderBy(F.col("qty_filled").desc_nulls_first(), "id")
w_sku_id_sku_is_active_by_qty_2 = Window.partitionBy("sku_id", "sku_is_active").orderBy(F.col("qty_filled").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter(F.col("move_type") != "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.qty_filled",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "qty_filled",
        "warehouse_wh_code",
        "sku_is_active",
        "sku_weight_kg",
        F.round(F.avg("sku_weight_kg").over(w_sku_is_active_by_qty_filled), 2).alias("sku_weight_kg_avg_1_prior"),
        F.row_number().over(w_sku_is_active_by_qty_filled_2).alias("seq_no"),
        F.max("sku_weight_kg").over(w_all_by_moved_at_prior_2).alias("sku_weight_kg_max_2_prior"),
        F.count(F.lit(1)).over(w_sku_id_operator).alias("rows_in_sku_id_operator"),
        F.size(F.collect_set("warehouse_id").over(w_operator_sku_id)).alias("distinct_warehouse_id_in_operator_sku_id"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_moved_at"),
        F.lag("moved_at").over(w_sku_id_sku_is_active_by_qty).alias("prev_moved_at"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_id_sku_is_active_by_qty_2).alias("second_sku_weight_kg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "qty_filled",
        "warehouse_wh_code",
        "sku_is_active",
        "sku_weight_kg_avg_1_prior",
        "seq_no",
        "sku_weight_kg_max_2_prior",
        "rows_in_sku_id_operator",
        "distinct_warehouse_id_in_operator_sku_id",
        "last_known_moved_at",
        "prev_moved_at",
        F.substring(F.trim(F.col("warehouse_wh_code")), 1, 3).alias("warehouse_wh_code_prefix"),
    )
)
