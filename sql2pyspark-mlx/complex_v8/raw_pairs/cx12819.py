from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("unit_price") >= 60.4) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

move_rows = df_stock_moves.select(F.col("id").alias("ref_id"), "moved_at", "warehouse_id")

move_rows_2 = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        F.col("warehouse_id").alias("ref_id"),
        "moved_at",
        F.col("sku_id").alias("warehouse_id"),
    )
)

result = (
    move_rows
    .unionByName(move_rows_2)
    .orderBy("moved_at")
)
