from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "OUTBOUND", "TRANSFER")) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

joined = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

filtered = (
    joined
    .crossJoin(joined.agg(F.max("warehouse_capacity").alias("overall_max_warehouse_capacity")))
    .filter(F.col("warehouse_capacity") < F.col("overall_max_warehouse_capacity"))
    .drop("overall_max_warehouse_capacity")
)

joined_sku = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.warehouse_opened_on",
        "s.warehouse_city",
        "s.warehouse_capacity",
        "s.sku_is_active",
        F.col("sk.is_active").alias("sku_is_active_2"),
        "sk.sku_code",
    )
)

enriched = (
    joined_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.warehouse_opened_on",
        "s.warehouse_capacity",
        "s.sku_is_active",
        "s.sku_code",
        F.col("sk.sku_code").alias("sku_sku_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

exploded = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_capacity",
        "sku_is_active",
        "sku_code",
        "sku_sku_code",
        "sku_weight_kg",
        F.explode("sku_labels").alias("sku_label"),
    )
)

result = (
    exploded
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_capacity",
        "sku_code",
        "sku_sku_code",
        "sku_weight_kg",
        "sku_label",
    )
    .orderBy(F.col("warehouse_capacity").desc(), "id", "sku_label")
    .limit(5)
)
