from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

move_side = (
    cleaned_moves
    .select(F.lit("move").alias("record_type"), F.col("qty").alias("entity_id"), "id")
)

move_side_2 = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")) & (F.col("qty") >= 198))
    .select(F.lit("move_2").alias("record_type"), "id", "warehouse_id")
)

combined = (
    move_side
    .union(move_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
