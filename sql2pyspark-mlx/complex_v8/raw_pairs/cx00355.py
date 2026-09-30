from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

move_type_stats = (
    base_moves
    .groupBy("move_type")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

compared = (
    base_moves.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    compared
    .groupBy("warehouse_id")
    .pivot("move_type", ["ADJUST", "TRANSFER", "INBOUND", "OUTBOUND"])
    .agg(F.max("avg_unit_price"))
    .orderBy("warehouse_id")
)
