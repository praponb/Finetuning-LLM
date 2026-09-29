from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 86.3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 290.4).alias("unit_price_distance"),
    )
)

with_children_moves = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    with_children_moves
    .groupBy("warehouse_id")
    .pivot("move_type", ["TRANSFER", "INBOUND", "ADJUST"])
    .agg(F.sum("move_unit_price"))
)
