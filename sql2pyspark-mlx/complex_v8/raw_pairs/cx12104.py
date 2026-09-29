from pyspark.sql import functions as F

recent = (
    df_skus
    .filter((F.col("unit_cost") > 134.3) & (F.col("category") == "Food"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "is_active",
        F.concat(F.col("category"), F.lit("#"), F.col("id").cast("string")).alias("category_ref"),
    )
)

sku_side = (
    recent
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
    .select("id")
)

move_side = (
    df_stock_moves
    .filter(F.col("operator") == "op_dew")
    .select("sku_id")
)

result = (
    sku_side
    .exceptAll(move_side)
    .orderBy(F.col("id").desc())
)
