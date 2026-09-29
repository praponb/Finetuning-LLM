from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.when(F.col("qty") >= 387, "A").when(F.col("qty") >= 123, "B").otherwise("C").alias("qty_tier"),
    )
)

derived = (
    base
    .filter(F.col("warehouse_id") >= 4)
    .select(
        "*",
        F.coalesce(F.col("operator_len"), F.lit(0)).alias("operator_len_filled"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

joined = (
    derived.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") != "Food"), "inner")
    .filter(F.col("sk.unit_cost") <= 251.2)
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_parent_warehouse = (
    joined.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price_bucket",
        "s.qty_tier",
        "s.operator_len_filled",
        "s.unit_price_adj",
        "s.operator_or_unknown",
        "s.sku_labels",
        "s.sku_weight_kg",
        "s.sku_unit_cost",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels_2"),
    )
)

result = (
    with_parent_warehouse
    .select(
        "id",
        "moved_at",
        "operator_len_filled",
        "sku_labels",
        "sku_weight_kg",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "warehouse_city",
        "sku_category",
        F.trunc(F.col("warehouse_opened_on"), "MM").alias("warehouse_opened_on_month_start"),
    )
    .orderBy(F.col("warehouse_wh_code").desc(), F.col("moved_at").desc_nulls_first())
)
