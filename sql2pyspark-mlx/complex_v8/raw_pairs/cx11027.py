from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_unit_price_tier = Window.partitionBy("operator", "unit_price_tier")
w_sku_id_unit_price_tier_by_mo = Window.partitionBy("sku_id", "unit_price_tier").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_sku_weight_kg_ties = Window.orderBy(F.col("sku_weight_kg").asc_nulls_last())
w_sku_id_unit_price_tier_by_mo_2 = Window.partitionBy("sku_id", "unit_price_tier").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_sku_weight_kg = Window.orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id")
w_all_by_warehouse_capacity = Window.orderBy("warehouse_capacity", "id")
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")

base = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(7, 9)) | (F.col("sku_id") >= 13))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 317.0, "A").when(F.col("unit_price") >= 189.4, "B").otherwise("C").alias("unit_price_tier"),
    )
)

joined_sku = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price_tier",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    joined_sku
    .select(
        "id",
        "sku_id",
        "moved_at",
        "sku_weight_kg",
        "sku_labels",
        F.min("warehouse_capacity").over(w_operator_unit_price_tier).alias("operator_unit_price_tier_min_warehouse_capacity"),
        F.round(F.avg("warehouse_capacity").over(w_sku_id_unit_price_tier_by_mo), 2).alias("warehouse_capacity_avg_4"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_operator_unit_price_tier), F.lit(0)), 4).alias("sku_weight_kg_share_of_operator_unit_price_tier"),
        F.sum("sku_weight_kg").over(w_operator_unit_price_tier).alias("operator_unit_price_tier_sum_sku_weight_kg"),
        F.round(F.percent_rank().over(w_all_by_sku_weight_kg_ties), 3).alias("sku_weight_kg_percent_rank"),
        F.round(F.avg("warehouse_capacity").over(w_sku_id_unit_price_tier_by_mo_2), 2).alias("warehouse_capacity_avg_2_prior"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("sku_weight_kg_share_of_move_type_warehouse_id"),
        F.lead("qty").over(w_all_by_sku_weight_kg).alias("next_qty"),
        (F.col("sku_weight_kg") - F.lag("sku_weight_kg").over(w_all_by_warehouse_capacity)).alias("sku_weight_kg_change"),
        F.nth_value("moved_at", 2).over(w_all_by_moved_at_full).alias("second_moved_at"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("warehouse_capacity_share_of_move_type_warehouse_"),
        F.count(F.lit(1)).over(w_move_type_warehouse_id).alias("rows_in_move_type_warehouse_id"),
        F.max("sku_weight_kg").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_sku_weight_kg"),
        F.round(F.avg("warehouse_capacity").over(w_move_type_warehouse_id), 2).alias("move_type_warehouse_id_avg_warehouse_capacity"),
    )
)

top_per_group = windowed.filter((F.col("rn") <= 5) & (F.size(F.col("sku_labels")) >= 2))

result = (
    top_per_group
    .select(
        "id",
        "sku_labels",
        "operator_unit_price_tier_min_warehouse_capacity",
        "warehouse_capacity_avg_4",
        "sku_weight_kg_share_of_operator_unit_price_tier",
        "operator_unit_price_tier_sum_sku_weight_kg",
        "sku_weight_kg_percent_rank",
        "sku_weight_kg_share_of_move_type_warehouse_id",
        "next_qty",
        "sku_weight_kg_change",
        "rn",
        "warehouse_capacity_share_of_move_type_warehouse_",
        "rows_in_move_type_warehouse_id",
        "move_type_warehouse_id_avg_warehouse_capacity",
        (F.col("sku_weight_kg_change") - F.col("sku_weight_kg")).alias("sku_weight_kg_change_minus_sku_weight_kg"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
