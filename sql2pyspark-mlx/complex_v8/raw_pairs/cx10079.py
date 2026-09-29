from pyspark.sql import functions as F

scoped_moves = (
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
    )
)

with_parent_sku = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

derived = (
    with_parent_sku
    .select(
        "*",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%ul%")), "left_semi")
)

with_parent_sku_2 = (
    without_matches.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") >= 68.0), "left")
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    with_parent_sku_2
    .select(
        "id",
        "warehouse_id",
        "unit_price",
        "sku_labels",
        "warehouse_opened_on",
        "unit_price_bucket",
        "sku_code",
        F.when(F.col("move_type").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.when(F.col("sku_is_active"), "yes").otherwise("no").alias("sku_is_active_label"),
    )
    .orderBy(F.col("warehouse_id").asc(), F.col("unit_price").desc_nulls_first())
)
