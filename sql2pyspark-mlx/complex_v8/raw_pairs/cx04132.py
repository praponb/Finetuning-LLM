from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (~F.col("move_type").isin("ADJUST", "TRANSFER")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

enriched = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") == "Food")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

annotated = enriched.withColumn("sku_category_clean", F.initcap(F.trim(F.col("sku_category"))))

with_details_sku = (
    annotated.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    with_details_sku
    .groupBy("sku_is_active", "warehouse_id")
    .pivot("sku_category", ["Apparel", "Toys", "Food"])
    .agg(F.sum("qty"))
)
