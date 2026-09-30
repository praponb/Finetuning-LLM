from pyspark.sql import functions as F

cleaned = (
    df_skus
    .filter(F.col("weight_kg") < 14.3)
    .select(
        "id",
        "sku_code",
        "category",
        "labels",
        F.substring(F.trim(F.col("category")), 1, 3).alias("category_prefix"),
    )
)

sku_side = cleaned.select(F.col("id").alias("entity_id"), F.col("category_prefix").alias("label"))

move_side = df_stock_moves.select("id", "operator")

result = (
    sku_side
    .subtract(move_side)
    .orderBy("entity_id", "label")
)
