from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("INBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

move_rows = (
    recent_moves
    .filter((F.col("operator") == "op_eve") & (F.col("qty").between(90, 426)))
    .select("warehouse_id", F.col("moved_at").alias("happened_at"), "move_type_group")
)

move_side = (
    df_stock_moves
    .filter((F.col("sku_id") <= 18) & (F.col("warehouse_id") >= 4))
    .select(
        F.col("qty").alias("warehouse_id"),
        F.col("moved_at").alias("happened_at"),
        F.col("move_type").alias("move_type_group"),
    )
)

result = (
    move_rows
    .union(move_side)
    .distinct()
)
