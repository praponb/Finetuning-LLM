from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("qty").between(124, 394))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

move_side = (
    prepared
    .filter((F.col("unit_price_bucket").between(101, 400)) & (F.col("unit_price") >= 180.0))
    .select(F.col("operator").alias("category"), "qty")
)

sku_side = df_skus.select("category", "id")

combined = (
    move_side
    .union(sku_side)
    .distinct()
)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("category")
)
