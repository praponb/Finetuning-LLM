from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_capacity = Window.orderBy(F.col("warehouse_capacity").desc(), "id")
w_move_type_qty_tier = Window.partitionBy("move_type", "qty_tier")
w_operator_warehouse_id_by_war = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id")
w_operator_warehouse_id_by_war_2 = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("warehouse_capacity").desc())
w_qty_tier_by_distinct_move_ty = Window.partitionBy("qty_tier").orderBy(F.col("distinct_move_type_in_moved_at_month_warehouse_i").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_tier_by_distinct_move_ty_2 = Window.partitionBy("qty_tier").orderBy(F.col("distinct_move_type_in_moved_at_month_warehouse_i").desc(), "id")
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_operator_in_move_ty = Window.partitionBy("distinct_operator_in_move_type_qty_tier", "operator")
w_all_by_moved_at = Window.orderBy("moved_at", "id")

recent = (
    df_stock_moves
    .filter((F.col("sku_id") <= 12) | (F.col("qty") >= 99))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.when(F.col("qty") >= 245, "large").when(F.col("qty") >= 109, "medium").otherwise("small").alias("qty_tier"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.qty_per_unit_price",
        "s.qty_tier",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.lag("moved_at", 3).over(w_all_by_warehouse_capacity).alias("prev3_moved_at"),
        F.count(F.lit(1)).over(w_move_type_qty_tier).alias("rows_in_move_type_qty_tier"),
        F.last("moved_at", ignorenulls=True).over(w_operator_warehouse_id_by_war).alias("last_known_moved_at"),
        F.round(F.col("qty_per_unit_price") - F.avg("qty_per_unit_price").over(w_move_type_qty_tier), 2).alias("qty_per_unit_price_vs_move_type_qty_tier_avg"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
        F.size(F.collect_set("move_type").over(w_moved_at_month_warehouse_id)).alias("distinct_move_type_in_moved_at_month_warehouse_i"),
        F.round(F.avg("qty_per_unit_price").over(w_move_type_qty_tier), 2).alias("move_type_qty_tier_avg_qty_per_unit_price"),
        F.round(F.avg("qty_per_unit_price").over(w_moved_at_month_warehouse_id), 2).alias("moved_at_month_warehouse_id_avg_qty_per_unit_pri"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_move_type_qty_tier), 2).alias("warehouse_capacity_vs_move_type_qty_tier_avg"),
        F.size(F.collect_set("operator").over(w_move_type_qty_tier)).alias("distinct_operator_in_move_type_qty_tier"),
        F.round(F.cume_dist().over(w_operator_warehouse_id_by_war_2), 3).alias("warehouse_capacity_cume_dist"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "qty_per_unit_price",
        "warehouse_capacity",
        "sku_labels",
        "rows_in_move_type_qty_tier",
        "last_known_moved_at",
        "qty_per_unit_price_vs_move_type_qty_tier_avg",
        "moved_at_cume_dist",
        "distinct_move_type_in_moved_at_month_warehouse_i",
        "move_type_qty_tier_avg_qty_per_unit_price",
        "moved_at_month_warehouse_id_avg_qty_per_unit_pri",
        F.sum("qty").over(w_qty_tier_by_distinct_move_ty).alias("running_qty"),
        F.round(F.percent_rank().over(w_qty_tier_by_distinct_move_ty_2), 3).alias("distinct_move_type_in_moved_at_month_warehouse_i_2"),
        F.nth_value("moved_at_cume_dist", 2).over(w_all_by_moved_at_full).alias("second_moved_at_cume_dist"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_distinct_operator_in_move_ty), 2).alias("warehouse_capacity_vs_distinct_operator_in_move_"),
        F.lead("qty").over(w_all_by_moved_at).alias("next_qty"),
        F.round(F.col("warehouse_capacity_cume_dist") - F.avg("warehouse_capacity_cume_dist").over(w_distinct_operator_in_move_ty), 2).alias("warehouse_capacity_cume_dist_vs_distinct_operato"),
    )
)

result = (
    windowed
    .select(
        "id",
        "qty",
        "moved_at",
        "qty_per_unit_price",
        "warehouse_capacity",
        "rows_in_move_type_qty_tier",
        "last_known_moved_at",
        "qty_per_unit_price_vs_move_type_qty_tier_avg",
        "moved_at_cume_dist",
        "distinct_move_type_in_moved_at_month_warehouse_i",
        "moved_at_month_warehouse_id_avg_qty_per_unit_pri",
        "running_qty",
        "distinct_move_type_in_moved_at_month_warehouse_i_2",
        "warehouse_capacity_vs_distinct_operator_in_move_",
        "next_qty",
        "warehouse_capacity_cume_dist_vs_distinct_operato",
    )
    .orderBy("next_qty")
)
