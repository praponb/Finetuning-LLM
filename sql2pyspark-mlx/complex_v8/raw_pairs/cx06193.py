from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
    )
)

move_side = (
    base_moves
    .select(
        "sku_id",
        F.col("moved_at").alias("happened_at"),
        F.col("id").alias("qty"),
    )
)

move_rows = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("operator") == "op_bee"))
    .select("warehouse_id", "moved_at", "qty")
)

all_events = move_side.union(move_rows)

result = (
    all_events
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("sku_id")
)
