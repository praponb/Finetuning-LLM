from pyspark.sql import functions as F

recent = (
    df_skus
    .filter((F.col("is_active")) & (F.col("unit_cost") < 277.0))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.round(F.col("weight_kg") * 1.07, 2).alias("weight_kg_adj"),
    )
)

sku_side = recent.select(F.col("id").alias("key_id"), "category")

warehouse_rows = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select("capacity", "city")
)

result = (
    sku_side
    .intersect(warehouse_rows)
    .orderBy("key_id")
)
