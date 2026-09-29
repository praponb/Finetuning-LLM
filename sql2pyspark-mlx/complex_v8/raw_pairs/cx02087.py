from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select("qty", "unit_price", F.lit("move").alias("origin"))
)

sku_rows = (
    df_skus
    .select(
        F.col("id").alias("qty"),
        F.col("weight_kg").alias("unit_price"),
        F.lit("sku").alias("origin"),
    )
)

stacked = (
    move_side
    .union(sku_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("origin")
)
