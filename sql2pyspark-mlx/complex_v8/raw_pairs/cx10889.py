from pyspark.sql import functions as F

prepared_skus = (
    df_skus
    .filter(F.col("sku_code").isNotNull())
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        F.concat_ws(": ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
    )
)

scored = (
    prepared_skus
    .filter(F.col("category_sku_code_label").isNotNull())
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "category_sku_code_label",
        F.lower(F.trim(F.col("category"))).alias("category_clean"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.unit_price") > 206.8) & (F.col("st.move_type").isin("TRANSFER", "OUTBOUND", "INBOUND")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "category_sku_code_label",
        "category_clean",
        F.lower(F.trim(F.col("category_sku_code_label"))).alias("category_sku_code_label_clean"),
        F.round(F.col("unit_cost") / F.nullif(F.col("weight_kg"), F.lit(0)), 3).alias("unit_cost_per_weight_kg"),
    )
    .orderBy(F.col("category_sku_code_label_clean").desc_nulls_first())
)
