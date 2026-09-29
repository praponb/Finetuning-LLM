from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (~F.col("move_type").isin("OUTBOUND", "TRANSFER")))
    .select(
        F.col("sku_id").alias("entity_id"),
        F.col("moved_at").alias("happened_at"),
        "unit_price",
    )
)

move_rows = (
    df_stock_moves
    .filter((F.col("sku_id") <= 5) & (F.col("operator") == "op_bee"))
    .select(
        F.col("sku_id").alias("entity_id"),
        F.col("moved_at").alias("happened_at"),
        "unit_price",
    )
)

stacked = (
    move_side
    .unionByName(move_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("entity_id")
)
