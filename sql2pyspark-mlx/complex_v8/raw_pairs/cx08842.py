from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_distinct_sku = Window.partitionBy(F.month("moved_at"), "distinct_sku_id_in_move_type")
w_warehouse_city_by_qty_full = Window.partitionBy("warehouse_city").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (~F.col("operator").isin("op_dew", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.abs(F.col("unit_price") - 290.4).alias("unit_price_distance"),
    )
)

without_matches = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") <= 306.4) & (~F.col("sk.is_active")), "left_semi")
)

combined_details = (
    without_matches.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("T%")), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.upper(F.trim(F.col("sk.sku_code"))).like("E%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.operator_len",
        "s.qty_bucket",
        "sk.sku_code",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type), F.lit(0)), 4).alias("qty_share_of_move_type"),
        F.nth_value("unit_price", 2).over(w_move_type_sku_id_by_unit_pri).alias("second_unit_price"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
    )
)

ranked = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "operator_len",
        "qty_bucket",
        "sku_code",
        "warehouse_wh_code",
        "qty_share_of_move_type",
        "second_unit_price",
        "distinct_sku_id_in_move_type",
        F.round(F.col("distinct_sku_id_in_move_type") - F.avg("distinct_sku_id_in_move_type").over(w_moved_at_month_distinct_sku), 2).alias("distinct_sku_id_in_move_type_vs_moved_at_month_d"),
        F.nth_value("second_unit_price", 2).over(w_warehouse_city_by_qty_full).alias("second_second_unit_price"),
        F.round(F.avg("second_unit_price").over(w_moved_at_month_distinct_sku), 2).alias("moved_at_month_distinct_sku_id_in_move_type_avg_"),
        F.count(F.lit(1)).over(w_moved_at_month_distinct_sku).alias("rows_in_moved_at_month_distinct_sku_id_in_move_t"),
        F.round(F.col("qty_share_of_move_type") / F.nullif(F.sum("qty_share_of_move_type").over(w_moved_at_month_distinct_sku), F.lit(0)), 4).alias("qty_share_of_move_type_share_of_moved_at_month_d"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") < 306.4), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "operator_len",
        "qty_bucket",
        "sku_code",
        "second_unit_price",
        "distinct_sku_id_in_move_type_vs_moved_at_month_d",
        "moved_at_month_distinct_sku_id_in_move_type_avg_",
        "rows_in_moved_at_month_distinct_sku_id_in_move_t",
        "qty_share_of_move_type_share_of_moved_at_month_d",
    )
)
