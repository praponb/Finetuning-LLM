from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

with_activity = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") <= 372), "left_anti")
)

derived = (
    with_activity
    .select(
        "*",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

joined_sku = (
    derived.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.qty_bucket",
        "s.operator_prefix",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

scored = (
    joined_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty_bucket",
        "operator_prefix",
        "sku_code",
        "sku_labels",
        "sku_weight_kg",
        "warehouse_wh_code",
        F.concat(F.col("warehouse_wh_code"), F.lit("#"), F.col("sku_id").cast("string")).alias("warehouse_wh_code_ref"),
        F.when(F.col("sku_weight_kg") >= 15.2, "A").when(F.col("sku_weight_kg") >= 12.4, "B").otherwise("C").alias("sku_weight_kg_tier"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty_bucket",
        "s.sku_code",
        "s.sku_labels",
        "s.sku_weight_kg",
        "s.warehouse_wh_code_ref",
        "s.sku_weight_kg_tier",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg_2"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty_bucket",
        "s.sku_code",
        "s.sku_weight_kg",
        "s.sku_weight_kg_2",
        "s.sku_is_active",
        F.col("sk.sku_code").alias("sku_sku_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg_3"),
        F.col("sk.labels").alias("sku_labels_2"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty_bucket",
        "sku_code",
        "sku_weight_kg",
        "sku_weight_kg_2",
        "sku_is_active",
        "sku_sku_code",
        "sku_labels_2",
        "warehouse_opened_on",
        F.round(F.col("sku_weight_kg_3") * 1.5, 2).alias("sku_weight_kg_3_adj"),
        (F.floor(F.col("qty_bucket") / 100) * 100).alias("qty_bucket_bucket"),
    )
    .orderBy("id")
)
