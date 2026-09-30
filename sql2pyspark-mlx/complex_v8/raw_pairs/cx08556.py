from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.days_since_moved_at",
        "s.moved_at_label",
        "sk.sku_code",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

joined_warehouse = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity").between(4912, 11768)), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.moved_at_label",
        "s.sku_code",
        "s.sku_category",
        "s.sku_weight_kg",
        "s.warehouse_capacity",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

enriched_2 = (
    joined_warehouse.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

rollup_base = (
    enriched_2
    .filter(F.col("warehouse_capacity") < 11005)
    .groupBy(
        "sku_category",
        F.when(F.col("warehouse_capacity") >= 11506, "A").when(F.col("warehouse_capacity") >= 4912, "B").otherwise("C").alias("warehouse_capacity_tier"),
        F.date_trunc("month", "warehouse_opened_on_2").alias("warehouse_opened_on_2_month"),
    )
    .agg(
        F.sum("sku_weight_kg").alias("total_sku_weight_kg"),
        F.sum("warehouse_capacity").alias("total_warehouse_capacity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    rollup_base
    .select(
        "sku_category",
        "total_sku_weight_kg",
        "total_warehouse_capacity",
        F.round(F.col("total_sku_weight_kg") / F.nullif(F.col("total_warehouse_capacity"), F.lit(0)), 3).alias("total_sku_weight_kg_per_total_warehouse_capacity"),
        (F.floor(F.col("total_sku_weight_kg") / 25) * 25).alias("total_sku_weight_kg_bucket"),
    )
    .orderBy(
        "total_warehouse_capacity",
        F.col("total_sku_weight_kg_per_total_warehouse_capacity").desc(),
    )
)
