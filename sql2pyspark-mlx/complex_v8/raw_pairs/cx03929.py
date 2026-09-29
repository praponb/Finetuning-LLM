from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("unit_price").between(111.7, 363.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

move_rows = (
    recent
    .filter((F.col("move_type").isNotNull()) & (F.col("warehouse_id").isNotNull()))
    .select("sku_id")
)

sku_rows = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "express")) & (F.length(F.trim(F.col("sku_code"))) >= 7))
    .select("id")
)

result = (
    move_rows
    .subtract(sku_rows)
    .orderBy("sku_id")
)
