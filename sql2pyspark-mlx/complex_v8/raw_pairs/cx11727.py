from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("qty") <= 392) & (F.col("moved_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price_minus_qty",
        "s.operator_move_type_label",
        "s.move_type_ref",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

combined_details = (
    with_parent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator_move_type_label",
        "s.move_type_ref",
        "s.sku_labels",
        "s.sku_category",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    combined_details
    .rollup("sku_category", "warehouse_city", "warehouse_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.upper(F.trim(F.col("warehouse_wh_code"))).like("K%"), 1).otherwise(0)).alias("n_upper_trim_warehouse_wh_code"),
        F.array_sort(F.collect_set("sku_category")).alias("sku_category_list"),
    )
    .orderBy(
        F.col("sku_category").asc_nulls_last(),
        F.col("warehouse_city").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
