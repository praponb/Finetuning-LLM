from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_bee", "op_ann")) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

combined_details_warehouse = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.moved_at_label",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        "sk.sku_code",
    )
)

annotated = (
    combined_details_warehouse
    .filter(F.col("moved_at_label").isNotNull())
    .select(
        "*",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.lower(F.regexp_replace(F.trim(F.col("moved_at_label")), " ", "_")).alias("moved_at_label_slug"),
        F.when(F.col("unit_price") >= 361.9, "HIGH").when(F.col("unit_price") >= 136.0, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

enriched = (
    annotated
    .select(
        "*",
        F.abs(F.col("warehouse_capacity") - 11506).alias("warehouse_capacity_distance"),
        F.coalesce(F.col("unit_price_tier"), F.lit("UNKNOWN")).alias("unit_price_tier_or_unknown"),
        (F.col("unit_price") + F.col("warehouse_capacity")).alias("unit_price_plus_warehouse_capacity"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.moved_at_label",
        "s.warehouse_wh_code",
        "s.sku_code",
        "s.unit_price_tier_or_unknown",
        "s.unit_price_plus_warehouse_capacity",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "warehouse_wh_code",
        "sku_code",
        "unit_price_plus_warehouse_capacity",
        "sku_is_active",
        F.upper(F.trim(F.col("warehouse_wh_code"))).alias("warehouse_wh_code_clean"),
        F.when(F.col("sku_is_active"), 1).otherwise(0).alias("sku_is_active_flag"),
    )
    .orderBy("warehouse_wh_code_clean", F.col("unit_price").desc(), "id")
    .limit(10)
)
