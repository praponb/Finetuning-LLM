from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_parent_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.col("sk.category") == "Food")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_parent_sku
    .groupBy("warehouse_city")
    .pivot("sku_category", ["Toys", "Food", "Electronics", "Beauty"])
    .agg(F.sum("qty"))
    .orderBy("warehouse_city")
)
