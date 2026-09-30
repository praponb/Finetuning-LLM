from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("move_type").isin("OUTBOUND", "ADJUST")) & (F.col("qty") <= 348))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.when(F.col("unit_price") >= 363.6, "large").when(F.col("unit_price") >= 152.7, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.substring(F.trim(F.col("unit_price_tier")), 1, 4).alias("unit_price_tier_prefix"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.concat_ws(": ", F.col("unit_price_tier"), F.col("move_type_operator_label")).alias("unit_price_tier_move_type_operator_label_label"),
    )
)

with_details_sku = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    with_details_sku
    .filter(F.col("qty") >= 148)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "days_since_moved_at",
        "move_type_operator_label",
        "unit_price_tier_prefix",
        "unit_price_adj",
        "sku_category",
        F.substring(F.trim(F.col("sku_category")), 1, 4).alias("sku_category_prefix"),
    )
    .orderBy(F.col("sku_category").desc(), F.col("days_since_moved_at").desc())
)
