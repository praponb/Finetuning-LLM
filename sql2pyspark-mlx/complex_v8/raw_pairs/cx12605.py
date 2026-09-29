from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("qty") < 256)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 387, "HIGH").when(F.col("qty") >= 148, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

with_children = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.move_type").isin("INBOUND", "OUTBOUND")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.qty_filled",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

result = (
    with_children
    .cube("move_warehouse_id", "move_type")
    .agg(F.min("unit_price").alias("min_unit_price"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("move_warehouse_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
