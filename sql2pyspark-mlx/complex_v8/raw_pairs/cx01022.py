from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("operator").alias("name"),
        F.col("move_type").alias("category"),
        "unit_price",
    )
)

sku_rows = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "cold")) & (F.col("weight_kg") <= 18.5))
    .select("sku_code", "category", "weight_kg")
)

stacked = (
    move_side
    .union(sku_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("name")
)
