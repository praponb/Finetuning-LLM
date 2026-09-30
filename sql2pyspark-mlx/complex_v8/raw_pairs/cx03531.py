from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

joined_warehouse = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price_adj",
        "s.moved_at_year",
        "s.unit_price_bucket",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

joined_sku = (
    joined_warehouse.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price_adj",
        "s.moved_at_year",
        "s.warehouse_wh_code",
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

joined_warehouse_2 = (
    joined_sku.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%k1%")), "inner")
    .filter(F.col("w.opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price_adj",
        "s.moved_at_year",
        "s.warehouse_wh_code",
        "s.sku_code",
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

labeled = (
    joined_warehouse_2
    .filter(F.upper(F.trim(F.col("warehouse_wh_code"))).like("B%"))
    .withColumn("warehouse_wh_code_2_sku_code_label", F.concat_ws(": ", F.col("warehouse_wh_code_2"), F.col("sku_code")))
)

result = (
    labeled
    .filter(F.col("warehouse_wh_code_2").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price_adj",
        "moved_at_year",
        "warehouse_wh_code",
        "sku_code",
        "warehouse_wh_code_2",
        "warehouse_opened_on",
        "warehouse_wh_code_2_sku_code_label",
    )
    .orderBy(F.col("warehouse_wh_code").desc_nulls_first(), "warehouse_id")
)
