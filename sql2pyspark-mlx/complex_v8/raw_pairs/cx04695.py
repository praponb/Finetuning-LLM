from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

summary = (
    cleaned
    .groupBy("warehouse_id", "sku_id", "move_type")
    .agg(
        F.round(100 * F.count(F.when(F.col("move_type") == "INBOUND", 1)) / F.count("*"), 1).alias("pct_move_type_inbound"),
        F.count("*").alias("n_rows"),
        F.min("qty").alias("min_qty"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "warehouse_id",
        "sku_id",
        "move_type",
        "pct_move_type_inbound",
        "n_rows",
        "min_qty",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        F.round(F.col("pct_move_type_inbound") * 1.07, 2).alias("pct_move_type_inbound_adj"),
    )
    .orderBy(F.col("pct_move_type_inbound_adj").desc(), F.col("n_rows").desc())
)
