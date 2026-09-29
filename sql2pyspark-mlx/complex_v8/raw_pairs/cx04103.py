from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.month(F.col("moved_at")).isin(2, 3)))
    .select(F.col("qty").alias("ref_id"), F.col("unit_price").alias("value"))
)

move_rows = (
    df_stock_moves
    .filter(F.col("unit_price") > 215.4)
    .select("id", "unit_price")
)

stacked = (
    move_side
    .union(move_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("ref_id")
)
