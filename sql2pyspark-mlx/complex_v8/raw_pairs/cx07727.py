from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(2, 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

move_side = (
    base
    .filter(F.col("unit_price") >= 119.5)
    .select(
        F.col("operator").alias("name"),
        "unit_price",
        "moved_at",
        F.lit("move").alias("src"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        "operator",
        "unit_price",
        "moved_at",
        F.lit("move_2").alias("src"),
    )
)

result = move_side.union(move_side_2)
