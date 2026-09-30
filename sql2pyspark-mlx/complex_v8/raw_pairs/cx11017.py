from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

annotated = (
    valid
    .select(
        "*",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
    )
)

sku_id_stats = (
    annotated
    .groupBy("sku_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

compared = (
    annotated.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "avg_qty",
        "qty_minus_avg",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy("warehouse_id", "id")
    .limit(10)
)
