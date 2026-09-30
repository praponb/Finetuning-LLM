from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

enriched = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") <= 15.2), "inner")
    .filter(F.lower(F.col("sk.sku_code")).like("%10%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "sk.sku_code",
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "sku_code",
    )
    .orderBy(F.col("id").asc(), "sku_code")
    .limit(10)
)
