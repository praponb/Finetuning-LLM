from pyspark.sql import functions as F

recent = (
    df_skus
    .filter(F.col("unit_cost") > 38.3)
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "labels",
    )
)

sku_rows = (
    recent
    .select(
        F.lit("sku").alias("origin"),
        F.col("id").alias("qty"),
        "sku_code",
        "unit_cost",
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.lit("move").alias("origin"),
        "id",
        "move_type",
        "unit_price",
    )
)

result = sku_rows.union(move_rows)
