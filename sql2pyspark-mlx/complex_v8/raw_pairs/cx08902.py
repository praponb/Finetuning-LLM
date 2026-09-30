from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("qty") > 168)
    .select(
        "operator",
        "unit_price",
        F.col("sku_id").alias("qty"),
        F.lit("move").alias("src"),
    )
)

sku_rows = (
    df_skus
    .filter(F.col("category") == "Toys")
    .select(
        F.col("category").alias("operator"),
        F.col("unit_cost").alias("unit_price"),
        F.col("id").alias("qty"),
        F.lit("sku").alias("src"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.col("unit_price") > 111.7) & (F.col("warehouse_id") >= 2))
    .select(
        F.col("move_type").alias("operator"),
        "unit_price",
        F.col("id").alias("qty"),
        F.lit("move_3").alias("src"),
    )
)

result = (
    move_side
    .union(sku_rows)
    .union(move_rows)
    .orderBy("unit_price")
)
