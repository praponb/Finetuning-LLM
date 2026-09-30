from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

enriched_sku = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.operator_prefix",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    enriched_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "operator_prefix",
        "sku_labels",
        "sku_unit_cost",
    )
    .orderBy("unit_price", F.col("warehouse_id").desc())
)
