from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("move_type") != "ADJUST"))
    .select("warehouse_id", F.col("unit_price").alias("metric"))
)

sku_side = (
    df_skus
    .filter((~F.array_contains(F.col("labels"), "hazmat")) & (~F.col("category").isin("Toys", "Electronics")))
    .select(F.col("id").alias("warehouse_id"), F.col("unit_cost").alias("metric"))
)

move_rows = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 4) & (F.col("qty") < 370))
    .select(
        F.col("sku_id").alias("warehouse_id"),
        F.col("unit_price").alias("metric"),
    )
)

stacked = (
    move_side
    .unionByName(sku_side)
    .unionByName(move_rows)
)

result = (
    stacked
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("warehouse_id")
)
