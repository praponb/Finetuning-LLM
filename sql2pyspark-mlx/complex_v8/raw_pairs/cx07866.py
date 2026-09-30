from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

operator_stats = (
    cleaned
    .groupBy("operator")
    .agg(F.avg("least_qty_unit_price").alias("avg_least_qty_unit_price"))
)

outliers = (
    cleaned.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.least_qty_unit_price") > F.col("g.avg_least_qty_unit_price"))
    .select(
        "s.*",
        "g.avg_least_qty_unit_price",
        F.round(F.col("s.least_qty_unit_price") - F.col("g.avg_least_qty_unit_price"), 2).alias("least_qty_unit_price_minus_avg"),
    )
)

move_type_stats = (
    outliers
    .groupBy("move_type")
    .agg(F.avg("least_qty_unit_price_minus_avg").alias("avg_least_qty_unit_price_minus_avg"))
)

compared = (
    outliers.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.least_qty_unit_price_minus_avg") > F.col("g.avg_least_qty_unit_price_minus_avg"))
    .select(
        "s.*",
        "g.avg_least_qty_unit_price_minus_avg",
        F.round(F.col("s.least_qty_unit_price_minus_avg") - F.col("g.avg_least_qty_unit_price_minus_avg"), 2).alias("least_qty_unit_price_minus_avg_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "avg_least_qty_unit_price",
        "avg_least_qty_unit_price_minus_avg",
        "least_qty_unit_price",
        "least_qty_unit_price_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["avg_least_qty_unit_price", "avg_least_qty_unit_price_minus_avg", "least_qty_unit_price", "least_qty_unit_price_minus_avg"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
