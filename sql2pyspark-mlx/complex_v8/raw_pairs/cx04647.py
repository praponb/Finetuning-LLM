from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

enriched_warehouse = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.qty_per_unit_price",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
    )
)

detail_rows = (
    enriched_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.qty_per_unit_price",
        "s.sku_category",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

combined_details_warehouse = (
    detail_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.move_warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.qty_per_unit_price",
        "s.sku_category",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_move_type",
        "s.move_moved_at",
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    combined_details_warehouse
    .groupingSets(
        [["move_move_type", "warehouse_city", "move_warehouse_id"], ["move_move_type"]],
        "move_move_type", "warehouse_city", "move_warehouse_id",
    )
    .agg(
        F.countDistinct("move_move_type").alias("distinct_move_move_types"),
        F.round(F.sum("qty") / F.nullif(F.sum("qty_per_unit_price"), F.lit(0)), 3).alias("qty_to_qty_per_unit_price_ratio"),
        F.max("move_moved_at").alias("last_move_moved_at"),
    )
    .orderBy(
        F.col("move_move_type").asc_nulls_last(),
        F.col("warehouse_city").asc_nulls_last(),
        F.col("move_warehouse_id").asc_nulls_last(),
    )
)
