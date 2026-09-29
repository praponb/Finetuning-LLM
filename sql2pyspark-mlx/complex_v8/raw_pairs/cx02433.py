from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year_by_moved_at_mo = Window.partitionBy(F.year("moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_operator_move_type = Window.partitionBy("operator", "move_type")

recent_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) | (F.col("unit_price").between(180.0, 307.8)))
)

with_trends = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.max("unit_price").over(w_moved_at_year_by_moved_at_mo).alias("unit_price_max_4"),
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
    )
)

enriched = (
    with_trends.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (~F.col("w.city").isin("Kuala Lumpur", "Bangkok")), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("w.wh_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_max_4",
        "s.rows_in_operator_move_type",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "unit_price_max_4",
        "rows_in_operator_move_type",
        "warehouse_capacity",
        "sku_weight_kg",
        F.abs(F.col("warehouse_capacity") - 4912).alias("warehouse_capacity_distance"),
        F.abs(F.col("sku_weight_kg") - 14.6).alias("sku_weight_kg_distance"),
    )
)
