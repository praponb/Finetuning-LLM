from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) & (F.col("operator") != "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.abs(F.col("qty") - 230).alias("qty_distance"),
    )
)

rollup_base = (
    valid_moves
    .groupBy(
        "move_type",
        F.when(F.col("unit_price") >= 289.7, "A").when(F.col("unit_price") >= 215.4, "B").otherwise("C").alias("unit_price_tier"),
        "sku_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("n_move_type_inbound"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.count(F.when(F.col("move_type") == "OUTBOUND", 1)).alias("n_move_type_outbound"),
        F.percentile_approx("operator_len", 0.5).alias("median_operator_len"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .groupBy("distinct_move_types")
    .pivot("move_type", ["OUTBOUND", "INBOUND"])
    .agg(F.min("median_operator_len"))
)
