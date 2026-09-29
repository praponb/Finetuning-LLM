from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_sku_id_by_sku = Window.partitionBy(F.month("moved_at"), "sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first(), "id")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_warehouse_id_moved_at_to_dat = Window.partitionBy("warehouse_id", F.to_date("moved_at")).orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_sku_id_by_sku_2 = Window.partitionBy(F.month("moved_at"), "sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first())
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

combined_details_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_ref",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

sequenced = (
    combined_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "operator_ref",
        "sku_weight_kg",
        F.ntile(4).over(w_moved_at_month_sku_id_by_sku).alias("sku_unit_cost_quartile"),
        F.sum("unit_price").over(w_move_type_operator).alias("move_type_operator_sum_unit_price"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_moved_at_to_dat).alias("second_moved_at"),
        F.round(F.cume_dist().over(w_moved_at_month_sku_id_by_sku_2), 3).alias("sku_unit_cost_cume_dist"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.max("unit_price").over(w_move_type_operator).alias("move_type_operator_max_unit_price"),
        F.lead("sku_unit_cost", 2).over(w_all_by_moved_at).alias("next2_sku_unit_cost"),
        F.size(F.collect_set("move_type").over(w_warehouse_id)).alias("distinct_move_type_in_warehouse_id"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_warehouse_id), 2).alias("sku_unit_cost_vs_warehouse_id_avg"),
        F.last("sku_weight_kg", ignorenulls=True).over(w_warehouse_id_move_type_by_mo).alias("last_known_sku_weight_kg"),
        F.max("sku_unit_cost").over(w_warehouse_id_move_type_by_mo_2).alias("sku_unit_cost_max_4"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.round(F.col("last_known_sku_weight_kg") * 0.25, 2).alias("last_known_sku_weight_kg_adj"),
        F.hour(F.col("second_moved_at")).alias("second_moved_at_hour"),
    )
)

result = (
    derived
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "sku_weight_kg",
        "sku_unit_cost_quartile",
        "move_type_operator_sum_unit_price",
        "second_moved_at",
        "sku_unit_cost_cume_dist",
        "unit_price_vs_move_type_operator_avg",
        "move_type_operator_max_unit_price",
        "next2_sku_unit_cost",
        "sku_unit_cost_vs_warehouse_id_avg",
        "last_known_sku_weight_kg",
        "sku_unit_cost_max_4",
        "last_known_sku_weight_kg_adj",
        "second_moved_at_hour",
        F.when(F.col("operator").isin("op_chan", "op_bee"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy("unit_price_vs_move_type_operator_avg", F.col("warehouse_id").desc())
)
