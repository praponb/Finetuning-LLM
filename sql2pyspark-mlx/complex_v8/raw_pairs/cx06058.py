from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

scored = (
    cleaned_moves
    .filter(F.col("unit_price") >= 61.2)
    .select(
        "*",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

with_details_warehouse = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price_filled",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

labeled = (
    with_details_warehouse
    .filter(~F.col("move_type").isin("OUTBOUND", "ADJUST"))
    .select(
        "*",
        F.coalesce(F.col("unit_price_filled"), F.lit(0.0)).alias("unit_price_filled_filled"),
        F.when(F.col("unit_price_filled") >= 350.3, "HIGH").when(F.col("unit_price_filled") >= 200.6, "MEDIUM").otherwise("LOW").alias("unit_price_filled_tier"),
    )
)

result = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price_filled",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "unit_price_filled_filled",
        "unit_price_filled_tier",
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
    .orderBy(F.col("unit_price_filled").desc(), F.col("sku_id").asc_nulls_last())
)
