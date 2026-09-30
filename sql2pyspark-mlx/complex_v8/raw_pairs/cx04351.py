from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.col("city") == "Singapore")
    .select(F.col("id").alias("qty"), F.col("wh_code").alias("label"))
)

move_side = (
    df_stock_moves
    .filter(F.col("operator").isin("op_chan", "op_bee", "op_eve"))
    .select("qty", F.col("move_type").alias("label"))
)

result = (
    warehouse_rows
    .unionByName(move_side)
    .distinct()
    .orderBy("label")
)
