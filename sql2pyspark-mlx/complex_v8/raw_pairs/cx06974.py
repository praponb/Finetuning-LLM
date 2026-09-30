from pyspark.sql import functions as F

move_side = df_stock_moves.select("operator", "move_type")

sku_side = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("A%"))
    .select(
        F.col("category").alias("operator"),
        F.col("sku_code").alias("move_type"),
    )
)

stacked = move_side.unionByName(sku_side)

result = (
    stacked
    .groupBy("operator")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("operator")
)
