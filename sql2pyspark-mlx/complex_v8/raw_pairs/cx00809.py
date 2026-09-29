from pyspark.sql import functions as F

move_side = df_stock_moves.select("qty", "operator", F.col("id").alias("entity_id"))

move_side_2 = (
    df_stock_moves
    .filter((F.col("operator") != "op_bee") & (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        F.col("sku_id").alias("qty"),
        F.col("move_type").alias("operator"),
        F.col("id").alias("entity_id"),
    )
)

result = (
    move_side
    .unionByName(move_side_2)
    .orderBy(F.col("qty").desc())
)
