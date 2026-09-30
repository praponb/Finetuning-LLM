from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter(F.size(F.col("labels")) >= 1)
    .select(
        F.lit("sku").alias("src"),
        "id",
        "unit_cost",
        "category",
    )
)

sku_side_2 = (
    df_skus
    .filter(F.array_contains(F.col("labels"), "express"))
    .select(
        F.lit("sku_2").alias("src"),
        "id",
        "unit_cost",
        "sku_code",
    )
)

result = (
    sku_side
    .union(sku_side_2)
    .orderBy("unit_cost", "id")
)
