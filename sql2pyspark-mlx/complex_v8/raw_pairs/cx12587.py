from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("qty").alias("ref_id"),
        F.col("sku_id").alias("qty"),
        F.col("unit_price").alias("metric"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (F.col("qty") > 109))
    .select(
        F.col("qty").alias("ref_id"),
        F.col("id").alias("qty"),
        F.col("unit_price").alias("metric"),
    )
)

stacked = move_side.union(move_rows)

result = (
    stacked
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("ref_id")
)
