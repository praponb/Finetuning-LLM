from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("qty") >= 191)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_details_sku = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

filtered = (
    with_details_sku
    .crossJoin(with_details_sku.agg(F.min("unit_price").alias("overall_min_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_min_unit_price"))
    .drop("overall_min_unit_price")
)

derived = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "sku_is_active",
        "sku_labels",
        F.size(F.col("sku_labels")).alias("sku_labels_count"),
    )
)

joined_sku = (
    derived.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.sku_code").isNotNull()), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.sku_is_active",
        "s.sku_labels_count",
        F.col("sk.labels").alias("sku_labels_2"),
    )
)

scored = (
    joined_sku
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "sku_is_active",
        "sku_labels_2",
        F.array_contains(F.col("sku_labels_2"), "bulky").alias("has_bulky"),
        F.round(F.col("qty") / F.nullif(F.col("sku_labels_count"), F.lit(0)), 3).alias("qty_per_sku_labels_count"),
    )
)

derived_2 = (
    scored
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "sku_is_active",
        "sku_labels_2",
        "qty_per_sku_labels_count",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

result = (
    derived_2
    .filter(F.col("operator_or_unknown") == "op_chan")
    .select(
        "id",
        "sku_id",
        "operator",
        "sku_is_active",
        "sku_labels_2",
        "qty_per_sku_labels_count",
        "qty_bucket",
    )
)
