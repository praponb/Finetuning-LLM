from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("qty") < 370)
    .select("sku_id", "unit_price")
)

move_rows_2 = df_stock_moves.select("sku_id", "unit_price")

unioned = (
    move_rows
    .unionByName(move_rows_2)
    .distinct()
)

result = (
    unioned
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("sku_id")
)
