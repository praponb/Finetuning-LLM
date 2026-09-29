from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select("id", F.col("wh_code").alias("name"))
)

move_side = df_stock_moves.select("warehouse_id", "move_type")

result = (
    warehouse_rows
    .subtract(move_side)
    .orderBy(F.col("id").desc())
)
