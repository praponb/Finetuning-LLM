from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("sku_id") <= 4) & (F.col("move_type") == "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_len",
        "s.unit_price_filled",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    enriched
    .groupingSets(
        [["move_type_len", "move_type", "warehouse_id"], ["move_type_len"]],
        "move_type_len", "move_type", "warehouse_id",
    )
    .agg(
        F.grouping("move_type_len").alias("is_total_move_type_len"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
    )
    .orderBy(
        F.col("move_type_len").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
