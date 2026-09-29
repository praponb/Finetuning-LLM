from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) | (F.col("sku_id") >= 16))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.when(F.col("unit_price") >= 363.6, "HIGH").when(F.col("unit_price") >= 136.0, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_qty",
        "st.last_moved_at",
        "st.max_move_qty",
    )
)

combined_details = (
    with_stats.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("sk.unit_cost").between(108.5, 277.0))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.least_unit_price_qty",
        "s.unit_price_tier",
        "s.avg_move_qty",
        "s.last_moved_at",
        "s.max_move_qty",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    combined_details
    .groupBy("sku_category")
    .pivot("operator", ["op_bee", "op_chan", "op_ann"])
    .agg(F.sum("sku_unit_cost"))
    .orderBy("sku_category")
)
