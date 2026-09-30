from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("qty") < 275))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_details_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.operator_prefix",
        "s.move_type_clean",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

enriched_sku = (
    with_details_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.operator_prefix",
        "s.sku_weight_kg",
        "s.sku_code",
        "s.warehouse_city",
        "s.warehouse_wh_code",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg_2"),
    )
)

annotated = (
    enriched_sku
    .withColumn("sku_weight_kg_minus_sku_weight_kg_2", F.col("sku_weight_kg") - F.col("sku_weight_kg_2"))
)

result = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator_prefix",
        "sku_weight_kg",
        "warehouse_city",
        "warehouse_wh_code",
        "sku_weight_kg_2",
        "sku_weight_kg_minus_sku_weight_kg_2",
        F.concat_ws("-", F.col("sku_code"), F.col("sku_category")).alias("sku_code_sku_category_label"),
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
    )
    .orderBy(
        F.col("moved_at_plus_1d").asc_nulls_last(),
        F.col("operator_prefix").desc(),
    )
)
