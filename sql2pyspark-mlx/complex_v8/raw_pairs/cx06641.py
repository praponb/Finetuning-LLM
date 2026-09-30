from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

joined_sku = (
    valid.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    joined_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "sku_labels",
        F.when(F.col("unit_price") >= 317.0, "HIGH").when(F.col("unit_price") >= 161.4, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)
