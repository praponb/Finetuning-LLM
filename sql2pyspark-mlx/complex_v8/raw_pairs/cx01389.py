from pyspark.sql import functions as F

sku_rows = df_skus.select(F.col("sku_code").alias("category"), F.col("unit_cost").alias("metric"))

move_side = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(7, 12))
    .select(
        F.col("move_type").alias("category"),
        F.col("unit_price").alias("metric"),
    )
)

result = (
    sku_rows
    .unionByName(move_side)
    .orderBy("category", "metric")
)
