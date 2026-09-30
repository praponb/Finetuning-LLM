from pyspark.sql import functions as F

valid = (
    df_skus
    .filter(F.col("category") != "Beauty")
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

sku_side = valid.select("id")

sku_rows = df_skus.select("id")

result = (
    sku_side
    .subtract(sku_rows)
    .orderBy("id")
)
