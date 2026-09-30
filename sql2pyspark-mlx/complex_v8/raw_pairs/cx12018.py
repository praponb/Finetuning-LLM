from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(~F.col("move_type").isin("ADJUST", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.when(F.col("unit_price") >= 361.9, "HIGH").when(F.col("unit_price") >= 206.8, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

move_side = (
    valid
    .filter(F.col("warehouse_id").isNotNull())
    .select(F.col("id").alias("entity_id"), F.col("unit_price_tier").alias("label"))
)

move_rows = df_stock_moves.select(F.col("id").alias("entity_id"), F.col("move_type").alias("label"))

result = (
    move_side
    .unionByName(move_rows)
    .distinct()
    .orderBy(F.col("label").desc(), "entity_id")
)
