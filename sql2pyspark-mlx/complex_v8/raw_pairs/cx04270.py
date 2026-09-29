from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
    )
)

move_rows = filtered.select(F.col("operator").alias("name"))

move_side = df_stock_moves.select(F.col("operator").alias("name"))

result = (
    move_rows
    .exceptAll(move_side)
    .orderBy("name")
)
