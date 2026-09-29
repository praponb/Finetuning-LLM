from pyspark.sql import functions as F

cleaned_skus = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "express")) & (F.col("is_active")))
    .select(
        "id",
        "category",
        "labels",
        "is_active",
    )
)

sku_rows = cleaned_skus.select("id")

sku_side = df_skus.select("id")

result = (
    sku_rows
    .subtract(sku_side)
    .orderBy("id")
)
