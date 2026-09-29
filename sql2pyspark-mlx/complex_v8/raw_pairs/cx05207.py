from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "warehouse_id",
        F.col("operator").alias("name"),
        "qty",
        F.lit("move").alias("src"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        "sku_id",
        "move_type",
        "warehouse_id",
        F.lit("move_2").alias("src"),
    )
)

move_rows_2 = (
    df_stock_moves
    .filter((F.col("unit_price") >= 119.5) & (~F.col("operator").isin("op_ann", "op_bee")))
    .select(
        "sku_id",
        "operator",
        "id",
        F.lit("move_3").alias("src"),
    )
)

result = (
    move_side
    .union(move_rows)
    .union(move_rows_2)
    .orderBy("name")
)
