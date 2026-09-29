from pyspark.sql import functions as F

cleaned = (
    df_skus
    .filter(F.col("sku_code").isNotNull())
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

sku_side = cleaned.select(F.col("id").alias("ref_id"), "unit_cost")

move_side = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select("sku_id", "unit_price")
)

result = (
    sku_side
    .union(move_side)
    .orderBy(F.col("unit_cost").desc())
)
