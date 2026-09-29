from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
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

joined_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
    )
)

joined_warehouse = (
    joined_sku.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.sku_labels",
        "s.sku_is_active",
        "s.sku_category",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.labels").alias("sku_labels_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("INBOUND", "ADJUST", "TRANSFER"), 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    joined_warehouse.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") < 11506), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.sku_labels",
        "s.sku_is_active",
        "s.warehouse_opened_on",
        "s.sku_labels_2",
        "s.move_move_type_count",
        F.col("w.city").alias("warehouse_city"),
    )
)

summary = (
    with_parent
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum(F.when(F.col("move_type") == "INBOUND", 1).otherwise(0)).alias("n_move_type_inbound"),
    )
)

result = (
    summary
    .filter(F.col("avg_unit_price") <= 350.3)
    .select("warehouse_id", "avg_unit_price", "n_move_type_inbound")
)
