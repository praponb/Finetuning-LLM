from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        F.lit("move").alias("record_type"),
        "sku_id",
        F.col("qty").alias("key_id"),
        F.col("unit_price").alias("metric"),
    )
)

move_rows_2 = (
    df_stock_moves
    .select(
        F.lit("move_2").alias("record_type"),
        "id",
        "warehouse_id",
        "unit_price",
    )
)

stacked = move_rows.union(move_rows_2)

result = (
    stacked
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("record_type")
)
