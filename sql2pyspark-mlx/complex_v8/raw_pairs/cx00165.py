from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("move_type").alias("label"),
        F.col("unit_price").alias("metric"),
        F.lit("move").alias("source"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.col("unit_price") >= 60.4) & (F.col("sku_id") >= 7))
    .select(
        F.col("operator").alias("label"),
        F.col("unit_price").alias("metric"),
        F.lit("move_2").alias("source"),
    )
)

stacked = (
    move_side
    .union(move_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("source")
)
