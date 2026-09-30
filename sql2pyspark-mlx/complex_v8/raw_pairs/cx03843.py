from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator") != "op_ann") & (F.col("qty") < 245))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_parent_sku = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_plus_qty",
        "s.operator_len",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.city").alias("warehouse_city"),
    )
)

tagged = (
    with_parent_sku
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "unit_price_plus_qty",
        "operator_len",
        "sku_weight_kg",
        "sku_category",
        "warehouse_city",
        F.length(F.trim(F.col("sku_category"))).alias("sku_category_len"),
    )
)

result = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "unit_price_plus_qty",
        "operator_len",
        "sku_weight_kg",
        "sku_category",
        "warehouse_city",
        "sku_category_len",
        F.coalesce(F.col("unit_price_plus_qty"), F.lit(0.0)).alias("unit_price_plus_qty_filled"),
        F.concat_ws("|", F.col("warehouse_city"), F.col("sku_category")).alias("warehouse_city_sku_category_label"),
    )
    .orderBy(F.col("warehouse_id").desc_nulls_first(), "id")
    .limit(20)
)
