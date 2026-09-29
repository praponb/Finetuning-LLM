from pyspark.sql import functions as F

scoped = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
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

sku_rows = (
    scoped
    .filter((F.col("weight_kg") > 6.5) & (F.col("is_active")))
    .select("id", F.col("category").alias("name"))
)

move_side = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select("id", "operator")
)

result = (
    sku_rows
    .exceptAll(move_side)
    .orderBy("name")
)
