from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

with_parent_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

combined_details = (
    with_parent_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.warehouse_opened_on",
        "s.warehouse_capacity",
        "s.warehouse_city",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "warehouse_opened_on",
        "warehouse_city",
        "warehouse_wh_code",
        "warehouse_opened_on_2",
        "sku_is_active",
        "sku_category",
    )
    .orderBy(F.col("warehouse_id").desc(), F.col("id").desc())
)
