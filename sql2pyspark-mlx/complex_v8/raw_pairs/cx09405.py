from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") >= 78) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

enriched = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.weight_kg").alias("sku_weight_kg"))
)

combined_details = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_or_unknown",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

tagged = (
    combined_details
    .filter(F.col("move_type_or_unknown") == "ADJUST")
    .select(
        "*",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
    )
)

with_details_sku = (
    tagged.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_or_unknown",
        "s.warehouse_opened_on",
        "s.unit_price_filled",
        "s.operator_len",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

rollup_base = (
    with_details_sku
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("move_type_or_unknown").alias("distinct_move_type_or_unknowns"),
        F.sum(F.when(F.col("sku_category") == "Apparel", F.col("warehouse_capacity")).otherwise(0)).alias("warehouse_capacity_sku_category_apparel"),
        F.min("warehouse_opened_on").alias("first_warehouse_opened_on"),
        F.sum(F.when(F.col("sku_category") == "Apparel", F.col("unit_price")).otherwise(0)).alias("unit_price_sku_category_apparel"),
        F.max("moved_at").alias("last_moved_at"),
        F.avg("unit_price").alias("avg_unit_price_check"),
    )
    .filter(F.col("avg_unit_price_check") > 205.1)
    .drop("avg_unit_price_check")
)

result = (
    rollup_base
    .select(
        "sku_id",
        "distinct_move_type_or_unknowns",
        "warehouse_capacity_sku_category_apparel",
        "first_warehouse_opened_on",
        "unit_price_sku_category_apparel",
        "last_moved_at",
        F.hour(F.col("last_moved_at")).alias("last_moved_at_hour"),
    )
    .orderBy(F.col("unit_price_sku_category_apparel").asc_nulls_last())
)
