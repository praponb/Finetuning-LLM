from pyspark.sql import functions as F

cleaned_moves = df_stock_moves.filter(F.col("qty") > 198)

move_side = cleaned_moves.select("id", F.col("unit_price").alias("metric"), "qty")

move_rows = (
    df_stock_moves
    .filter(F.col("qty") >= 90)
    .select(F.col("sku_id").alias("id"), F.col("unit_price").alias("metric"), "qty")
)

move_rows_2 = (
    df_stock_moves
    .select(
        "id",
        F.col("unit_price").alias("metric"),
        F.col("warehouse_id").alias("qty"),
    )
)

stacked = (
    move_side
    .unionByName(move_rows)
    .unionByName(move_rows_2)
)

result = (
    stacked
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("id")
)
