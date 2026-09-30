from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 275) & (F.col("unit_price") >= 200.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 256, "A").when(F.col("qty") >= 109, "B").otherwise("C").alias("qty_tier"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_details_warehouse = (
    recent_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_parent_warehouse = (
    with_details_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.length(F.trim(F.col("w.wh_code"))) >= 8)
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    with_parent_warehouse
    .rollup("warehouse_id", "sku_category", "qty_tier")
    .agg(
        F.round(100 * F.count(F.when(~F.col("sku_is_active"), 1)) / F.count("*"), 1).alias("pct_sku_is_active_false"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_category").asc_nulls_last(),
        F.col("qty_tier").asc_nulls_last(),
    )
)
