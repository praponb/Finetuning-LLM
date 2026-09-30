from pyspark.sql import functions as F

valid_skus = (
    df_skus
    .filter((F.col("sku_code").isNotNull()) & (F.col("category") == "Beauty"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

sku_side = valid_skus.select(F.col("id").alias("ref_id"), "unit_cost")

move_side = (
    df_stock_moves
    .select(
        F.col("warehouse_id").alias("ref_id"),
        F.col("unit_price").alias("unit_cost"),
    )
)

result = sku_side.union(move_side)
