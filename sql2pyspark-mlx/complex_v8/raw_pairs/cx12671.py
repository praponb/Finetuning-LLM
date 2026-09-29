from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("operator").isin("op_chan", "op_bee", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

move_rows = (
    recent
    .filter(F.col("unit_price") > 215.4)
    .select(F.col("sku_id").alias("key_id"), F.col("move_type").alias("name"))
)

move_side = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") > 238.5))
    .select(F.col("id").alias("key_id"), F.col("operator").alias("name"))
)

result = (
    move_rows
    .unionByName(move_side)
    .distinct()
    .orderBy(F.col("name").desc(), "key_id")
)
