from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator").isin("op_eve", "op_dew", "op_chan")) & (F.col("warehouse_id") <= 3))
)

with_parent_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_parent_warehouse
    .groupingSets(
        [["warehouse_id", "warehouse_city", "move_type"], ["warehouse_id"], ["warehouse_city"], []],
        "warehouse_id", "warehouse_city", "move_type",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("operator") == "op_bee", 1)) / F.count("*"), 1).alias("pct_operator_op_bee"),
    )
    .select(
        "warehouse_id",
        F.coalesce(F.col("warehouse_city"), F.lit("ALL")).alias("warehouse_city"),
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "pct_operator_op_bee",
    )
)
