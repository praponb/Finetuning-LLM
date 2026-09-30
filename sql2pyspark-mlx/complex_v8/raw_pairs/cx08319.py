from pyspark.sql import functions as F

recent = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "is_active",
    )
)

sku_side = (
    recent
    .filter((F.col("category").isNotNull()) & (F.col("unit_cost").between(108.5, 330.6)))
    .select("category", F.col("id").alias("qty"), F.col("unit_cost").alias("amount"))
)

move_side = (
    df_stock_moves
    .select(
        F.col("move_type").alias("category"),
        F.col("id").alias("qty"),
        F.col("unit_price").alias("amount"),
    )
)

sku_rows = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "bulky")) & (F.col("category").isNotNull()))
    .select("category", F.col("id").alias("qty"), F.col("weight_kg").alias("amount"))
)

result = (
    sku_side
    .unionByName(move_side)
    .unionByName(sku_rows)
)
