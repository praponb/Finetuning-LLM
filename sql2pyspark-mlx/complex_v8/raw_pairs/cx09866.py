from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_sku_is_active_by_sku_weight = Window.partitionBy("sku_is_active").orderBy("sku_weight_kg")
w_all_by_qty_running = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_sku_is_active_by_move = Window.partitionBy("sku_id", "sku_is_active").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_moves = (
    df_stock_moves
    .filter((F.col("qty") < 372) & (F.col("unit_price").between(73.0, 330.9)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

enriched_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.weight_kg") < 15.2)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_windows = (
    enriched_sku
    .select(
        "*",
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_warehouse_id), 2).alias("sku_unit_cost_vs_warehouse_id_avg"),
        F.nth_value("sku_weight_kg", 2).over(w_all_by_qty_full).alias("second_sku_weight_kg"),
        (F.col("sku_weight_kg") - F.lag("sku_weight_kg").over(w_all_by_qty)).alias("sku_weight_kg_change"),
        F.round(F.percent_rank().over(w_sku_is_active_by_sku_weight), 3).alias("sku_weight_kg_percent_rank"),
        F.max("sku_weight_kg").over(w_warehouse_id).alias("warehouse_id_max_sku_weight_kg"),
        F.sum("sku_unit_cost").over(w_warehouse_id).alias("warehouse_id_sum_sku_unit_cost"),
        F.first("moved_at", ignorenulls=True).over(w_all_by_qty_running).alias("first_known_moved_at"),
        F.nth_value("sku_weight_kg", 2).over(w_sku_id_sku_is_active_by_move).alias("second_sku_weight_kg_2"),
        F.ntile(3).over(w_all_by_qty).alias("qty_tercile"),
        F.size(F.collect_set("operator").over(w_warehouse_id)).alias("distinct_operator_in_warehouse_id"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.rank().over(w_sku_is_active_by_sku_weight).alias("sku_weight_kg_rank"),
        F.min("sku_unit_cost").over(w_warehouse_id).alias("warehouse_id_min_sku_unit_cost"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_id), 2).alias("warehouse_id_avg_sku_weight_kg"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        (F.col("second_sku_weight_kg_2") - F.col("sku_weight_kg")).alias("second_sku_weight_kg_2_minus_sku_weight_kg"),
        F.round(F.col("second_sku_weight_kg_2") * 0.25, 2).alias("second_sku_weight_kg_2_adj"),
    )
)

result = (
    annotated
    .filter(F.col("operator") == "op_dew")
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "sku_weight_kg",
        "sku_is_active",
        "second_sku_weight_kg",
        "sku_weight_kg_percent_rank",
        "warehouse_id_max_sku_weight_kg",
        "warehouse_id_sum_sku_unit_cost",
        "qty_tercile",
        "distinct_sku_id_in_warehouse_id",
        "sku_weight_kg_rank",
        "warehouse_id_min_sku_unit_cost",
        "warehouse_id_avg_sku_weight_kg",
        "second_sku_weight_kg_2_minus_sku_weight_kg",
        F.coalesce(F.col("second_sku_weight_kg"), F.lit(0.0)).alias("second_sku_weight_kg_filled"),
    )
    .orderBy(F.col("sku_weight_kg_percent_rank").desc_nulls_first())
)
