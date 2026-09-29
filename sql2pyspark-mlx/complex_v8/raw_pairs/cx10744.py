from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

result = (
    recent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
    )
    .orderBy("qty")
)
