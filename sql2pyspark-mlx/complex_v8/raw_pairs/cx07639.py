from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(
        "wh_code",
        F.col("id").alias("ref_id"),
        F.lit("warehouse").alias("source"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.col("operator").alias("wh_code"),
        F.col("id").alias("ref_id"),
        F.lit("move").alias("source"),
    )
)

result = (
    warehouse_rows
    .unionByName(move_rows)
    .distinct()
)
