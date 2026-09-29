from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        F.lit("warehouse").alias("source"),
        F.col("id").alias("ref_id"),
        "city",
        "capacity",
    )
)

move_side = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("unit_price") < 393.3))
    .select(
        F.lit("move").alias("source"),
        F.col("sku_id").alias("ref_id"),
        F.col("operator").alias("city"),
        F.col("warehouse_id").alias("capacity"),
    )
)

all_events = warehouse_side.unionByName(move_side)

result = (
    all_events
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("source")
)
