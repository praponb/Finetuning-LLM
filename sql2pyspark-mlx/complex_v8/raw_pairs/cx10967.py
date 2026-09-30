from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("qty") >= 89) | (F.col("sku_id") <= 8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

screened = (
    filtered
    .crossJoin(filtered.agg(F.avg("qty_minus_unit_price").alias("overall_avg_qty_minus_unit_price")))
    .filter(F.col("qty_minus_unit_price") > F.col("overall_avg_qty_minus_unit_price"))
    .drop("overall_avg_qty_minus_unit_price")
)

combined_details = (
    screened.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.greatest_unit_price_qty",
        "s.move_type_ref",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    combined_details
    .cube("sku_id", "warehouse_city")
    .agg(
        F.max("greatest_unit_price_qty").alias("max_greatest_unit_price_qty"),
        F.count("*").alias("row_count"),
        F.sum("greatest_unit_price_qty").alias("total_greatest_unit_price_qty"),
    )
    .select(
        "sku_id",
        F.coalesce(F.col("warehouse_city"), F.lit("ALL")).alias("warehouse_city"),
        "max_greatest_unit_price_qty",
        "row_count",
        "total_greatest_unit_price_qty",
    )
)
