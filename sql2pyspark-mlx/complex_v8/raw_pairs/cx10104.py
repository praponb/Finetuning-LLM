from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_all_by_unit_price_adj = Window.orderBy("unit_price_adj", "id")
w_all_by_unit_price_adj_full = Window.orderBy("unit_price_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")

scoped = (
    df_stock_moves
    .filter((~F.col("move_type").isin("INBOUND", "OUTBOUND")) & (F.col("qty") > 189))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

sequenced = (
    qualified
    .select(
        "*",
        F.lead("unit_price_adj", 1, 0).over(w_all_by_unit_price).alias("next_unit_price_adj"),
        F.row_number().over(w_all_by_unit_price_adj).alias("rn"),
        F.first("unit_price").over(w_all_by_unit_price_adj_full).alias("first_unit_price"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.size(F.collect_set("operator").over(w_warehouse_id_move_type)).alias("distinct_operator_in_warehouse_id_move_type"),
    )
)

with_parent_warehouse = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_adj",
        "s.operator_move_type_label",
        "s.next_unit_price_adj",
        "s.rn",
        "s.first_unit_price",
        "s.rows_in_warehouse_id_move_type",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

grouped = (
    with_parent_warehouse
    .groupBy("rn", "sku_id", "warehouse_id")
    .agg(
        F.min("warehouse_opened_on").alias("first_warehouse_opened_on"),
        F.round(F.sum("warehouse_capacity") / F.nullif(F.sum("rn"), F.lit(0)), 3).alias("warehouse_capacity_to_rn_ratio"),
        F.avg("unit_price_adj").alias("avg_unit_price_adj_check"),
    )
    .filter(F.col("avg_unit_price_adj_check") > 300.6)
    .drop("avg_unit_price_adj_check")
)

result = (
    grouped
    .groupingSets(
        [["rn", "sku_id", "warehouse_id"], ["rn"], ["sku_id"], []],
        "rn", "sku_id", "warehouse_id",
    )
    .agg(
        F.min("first_warehouse_opened_on").alias("first_first_warehouse_opened_on"),
    )
)
