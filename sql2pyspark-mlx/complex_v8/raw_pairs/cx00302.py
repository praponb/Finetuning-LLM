from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "qty",
        "id",
        "move_type",
        F.lit("move").alias("src"),
    )
)

warehouse_side = (
    df_warehouses
    .filter(F.col("capacity") < 11506)
    .select(
        "id",
        "capacity",
        "wh_code",
        F.lit("warehouse").alias("src"),
    )
)

combined = move_side.union(warehouse_side)

result = (
    combined
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("src")
)
