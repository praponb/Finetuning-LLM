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
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

enriched = (
    valid_moves
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_per_qty",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean_2"),
    )
)

combined_details_sku = (
    enriched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.weight_kg").alias("sku_weight_kg"))
)

result = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price_per_qty",
        "move_type_clean",
        "operator_prefix",
        F.when(F.col("unit_price") >= 307.8, "large").when(F.col("unit_price") >= 238.5, "medium").otherwise("small").alias("unit_price_tier"),
        F.length(F.trim(F.col("move_type_clean_2"))).alias("move_type_clean_2_len"),
    )
    .orderBy(F.col("unit_price_tier").desc())
)
