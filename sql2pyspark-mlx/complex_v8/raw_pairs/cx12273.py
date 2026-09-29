from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("city") == "Bangkok"))
    .select(
        F.lit("warehouse").alias("source"),
        F.col("capacity").alias("entity_id"),
        F.col("id").alias("qty"),
    )
)

move_side = df_stock_moves.select(F.lit("move").alias("source"), "sku_id", "qty")

stacked = warehouse_side.union(move_side)

result = (
    stacked
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("source")
)
