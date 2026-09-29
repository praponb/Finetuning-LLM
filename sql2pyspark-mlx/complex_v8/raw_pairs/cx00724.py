from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("qty") > 78)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 227).alias("qty_distance"),
    )
)

with_details_sku = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.qty_distance",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

joined = (
    with_details_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.qty_distance",
        "s.warehouse_wh_code",
        "s.warehouse_city",
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
    )
)

result = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "qty_distance",
        "warehouse_wh_code",
        "warehouse_city",
        "sku_code",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
        F.when(F.col("qty") >= 349, "A").when(F.col("qty") >= 152, "B").otherwise("C").alias("qty_tier"),
    )
    .orderBy(F.col("qty_distance").asc(), F.col("sku_id").desc())
)
