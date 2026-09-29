from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("qty") > 99)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

enriched_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.size(F.col("sk.labels")) >= 1), "inner")
    .filter(F.col("sk.weight_kg") >= 4.7)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_adj",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

filtered = (
    enriched_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") <= 5), "left_semi")
)

annotated = (
    filtered
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "*",
        F.when(F.col("qty") >= 271, "A").when(F.col("qty") >= 169, "B").otherwise("C").alias("qty_tier"),
        F.round(F.col("unit_price_adj") * 0.9, 2).alias("unit_price_adj_adj"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

result = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "qty",
        "unit_price",
        "sku_weight_kg",
        "qty_tier",
        "unit_price_adj_adj",
        F.round(F.col("sku_weight_kg") / F.nullif(F.col("unit_price_adj"), F.lit(0)), 3).alias("sku_weight_kg_per_unit_price_adj"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
    .orderBy(F.col("qty").desc())
)
