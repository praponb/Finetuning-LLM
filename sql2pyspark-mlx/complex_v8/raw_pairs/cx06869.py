from pyspark.sql import functions as F

base_skus = (
    df_skus
    .filter((F.upper(F.trim(F.col("sku_code"))).like("T%")) | (~F.array_contains(F.col("labels"), "cold")))
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "is_active",
        (F.col("unit_cost") + F.col("weight_kg")).alias("unit_cost_plus_weight_kg"),
    )
)

sku_side = base_skus.select("id", "sku_code")

move_rows = (
    df_stock_moves
    .filter((F.col("unit_price") < 399.6) & (F.month(F.col("moved_at")).isin(2, 3)))
    .select("id", "operator")
)

result = (
    sku_side
    .exceptAll(move_rows)
    .orderBy("sku_code", "id")
)
