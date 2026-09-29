from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("category") == "Beauty") & (~F.col("is_active")))
    .select("id")
)

sku_side_2 = (
    df_skus
    .filter(F.col("is_active"))
    .select("id")
)

result = (
    sku_side
    .subtract(sku_side_2)
    .orderBy(F.col("id").desc())
)
