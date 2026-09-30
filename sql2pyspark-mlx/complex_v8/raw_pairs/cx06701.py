from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("unit_price").between(130.6, 293.2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

result = (
    prepared_moves
    .filter(F.col("move_type").isin("OUTBOUND", "INBOUND", "TRANSFER"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "least_unit_price_qty",
    )
    .orderBy("qty", "move_type")
)
