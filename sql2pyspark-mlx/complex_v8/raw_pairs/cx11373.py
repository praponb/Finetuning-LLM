from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 189.4).alias("unit_price_distance"),
    )
)

joined_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

enriched = (
    joined_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.warehouse_opened_on",
        "s.warehouse_city",
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "warehouse_opened_on",
        "warehouse_city",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("warehouse_opened_on")).alias("days_since_warehouse_opened_on"),
    )
)
