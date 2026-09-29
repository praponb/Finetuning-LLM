from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(~F.col("move_type").isin("INBOUND", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

move_side = (
    cleaned
    .select(
        F.col("operator").alias("label"),
        "moved_at",
        F.lit("move").alias("record_type"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select("operator", "moved_at", F.lit("move_2").alias("record_type"))
)

combined = (
    move_side
    .union(move_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("record_type")
)
