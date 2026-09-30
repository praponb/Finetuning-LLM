from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.col("qty") <= 355) | (~F.col("operator").isin("op_eve", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
    )
)

move_side = (
    prepared
    .select(
        F.lit("move").alias("src"),
        F.col("sku_id").alias("entity_id"),
        F.col("warehouse_id").alias("qty"),
    )
)

move_rows = df_stock_moves.select(F.lit("move_2").alias("src"), "id", "warehouse_id")

combined = move_side.union(move_rows)

result = (
    combined
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("src")
)
