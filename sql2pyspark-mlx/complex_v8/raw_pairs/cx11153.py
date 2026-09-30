from pyspark.sql import functions as F

move_rows = df_stock_moves.select("operator")

warehouse_side = (
    df_warehouses
    .filter((F.col("capacity") < 11768) & (F.col("city") != "Kuala Lumpur"))
    .select(F.col("wh_code").alias("operator"))
)

result = (
    move_rows
    .exceptAll(warehouse_side)
    .orderBy("operator")
)
