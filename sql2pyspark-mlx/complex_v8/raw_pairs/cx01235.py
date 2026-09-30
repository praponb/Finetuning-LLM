from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("unit_price") >= 119.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

qualified = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") != "TRANSFER") & (F.col("st.moved_at") < F.lit("2025-03-02 06:00:00").cast("timestamp")), "left_semi")
)

result = (
    qualified
    .rollup("sku_id", "warehouse_id", "move_type")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("moved_at").alias("first_moved_at"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
