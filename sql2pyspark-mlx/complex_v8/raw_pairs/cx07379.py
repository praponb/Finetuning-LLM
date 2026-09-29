from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        F.lit("move").alias("source"),
        F.col("qty").alias("ref_id"),
        F.col("id").alias("key_id"),
    )
)

move_side = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        F.lit("move_2").alias("source"),
        F.col("qty").alias("ref_id"),
        F.col("sku_id").alias("key_id"),
    )
)

result = move_rows.unionByName(move_side)
