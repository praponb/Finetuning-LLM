from pyspark.sql import functions as F

recent_skus = (
    df_skus
    .filter(F.col("weight_kg") >= 13.6)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
    )
)

sku_side = (
    recent_skus
    .filter((F.col("weight_kg") > 8.5) & (F.col("unit_cost").between(38.3, 306.4)))
    .select("id", F.col("unit_cost").alias("metric"))
)

move_side = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select("qty", "unit_price")
)

result = (
    sku_side
    .union(move_side)
    .distinct()
    .orderBy("metric", "id")
)
