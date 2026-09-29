from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

screened = (
    prepared_moves
    .crossJoin(prepared_moves.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

rollup_base = (
    screened
    .filter(F.col("sku_id") >= 18)
    .groupBy("move_type")
    .agg(
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.sum(F.when(F.col("operator").isin("op_ann", "op_eve", "op_dew"), F.col("unit_price")).otherwise(0)).alias("unit_price_operator_in_op_ann_op_eve_op"),
        F.count("*").alias("row_count"),
    )
)

result = (
    rollup_base
    .groupBy("row_count", "distinct_move_types")
    .pivot("move_type", ["ADJUST", "INBOUND", "OUTBOUND"])
    .agg(F.avg("unit_price_operator_in_op_ann_op_eve_op").alias("avg_v"), F.sum("unit_price_operator_in_op_ann_op_eve_op").alias("sum_v"))
    .orderBy("row_count", "distinct_move_types")
)
