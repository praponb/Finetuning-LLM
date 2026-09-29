from pyspark.sql import functions as F

move_rows = df_stock_moves.select(F.col("sku_id").alias("key_id"), "operator")

move_side = df_stock_moves.select(F.col("qty").alias("key_id"), F.col("move_type").alias("operator"))

warehouse_rows = (
    df_warehouses
    .filter((F.lower(F.col("wh_code")).like("%x1%")) & (F.col("capacity") > 4912))
    .select(F.col("id").alias("key_id"), F.col("wh_code").alias("operator"))
)

stacked = (
    move_rows
    .unionByName(move_side)
    .unionByName(warehouse_rows)
)

result = (
    stacked
    .groupBy("operator")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("operator")
)
