from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id", "wh_code", F.lit("warehouse").alias("src"))

move_side = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        F.col("sku_id").alias("id"),
        F.col("operator").alias("wh_code"),
        F.lit("move").alias("src"),
    )
)

all_events = warehouse_rows.union(move_side)

result = (
    all_events
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("src")
)
