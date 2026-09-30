from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.col("city").isin("Singapore", "Kuala Lumpur"))
    .select("capacity", F.col("id").alias("ref_id"))
)

move_side = (
    df_stock_moves
    .filter((F.col("sku_id") <= 14) & (~F.col("operator").isin("op_chan", "op_ann")))
    .select(F.col("qty").alias("capacity"), F.col("sku_id").alias("ref_id"))
)

result = (
    warehouse_rows
    .unionByName(move_side)
    .distinct()
)
