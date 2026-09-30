from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select("operator")
)

sku_side = (
    df_skus
    .filter(F.col("is_active"))
    .select("sku_code")
)

result = (
    move_side
    .intersectAll(sku_side)
    .orderBy("operator")
)
