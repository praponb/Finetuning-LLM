from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "sku_id",
        "id",
        F.col("unit_price").alias("value"),
        F.lit("move").alias("origin"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "sku_id",
        F.col("qty").alias("id"),
        F.col("unit_price").alias("value"),
        F.lit("move_2").alias("origin"),
    )
)

result = (
    move_side
    .unionByName(move_side_2)
    .distinct()
    .orderBy("id")
)
