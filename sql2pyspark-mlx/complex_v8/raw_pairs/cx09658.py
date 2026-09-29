from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

scored = (
    base
    .select(
        "*",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.when(F.col("unit_price") >= 298.9, "large").when(F.col("unit_price") >= 150.7, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

move_type_stats = (
    scored
    .groupBy("move_type")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

outliers = (
    scored.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "avg_unit_price",
        "unit_price_minus_avg",
        "unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["avg_unit_price", "unit_price_minus_avg", "unit_price"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
