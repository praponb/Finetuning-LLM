from pyspark.sql import functions as F

prepared = (
    df_skus
    .filter((F.col("unit_cost").between(17.3, 251.2)) & (F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        F.substring(F.trim(F.col("sku_code")), 1, 3).alias("sku_code_prefix"),
    )
)

sku_side = (
    prepared
    .filter((F.col("sku_code").isNotNull()) & (F.col("category").isNotNull()))
    .select("id")
)

warehouse_side = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select("id")
)

result = sku_side.intersectAll(warehouse_side)
