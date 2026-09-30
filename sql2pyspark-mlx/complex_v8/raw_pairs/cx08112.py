from pyspark.sql import functions as F

joined_ranges = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.qty").alias("prior_qty_1d"),
    )
)

sku_id_stats = (
    joined_ranges
    .groupBy("sku_id")
    .agg(F.percentile_approx("prior_qty_1d", 0.5).alias("median_prior_qty_1d"))
)

vs_group = (
    joined_ranges.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.prior_qty_1d") >= F.col("g.median_prior_qty_1d"))
    .select(
        "s.*",
        "g.median_prior_qty_1d",
        F.round(F.col("s.prior_qty_1d") - F.col("g.median_prior_qty_1d"), 2).alias("prior_qty_1d_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_1d",
        "median_prior_qty_1d",
        F.round(F.col("median_prior_qty_1d") / F.nullif(F.col("prior_qty_1d"), F.lit(0)), 3).alias("median_prior_qty_1d_per_prior_qty_1d"),
        (F.col("prior_qty_1d") + F.col("prior_rows_1d")).alias("prior_qty_1d_plus_prior_rows_1d"),
    )
    .orderBy("median_prior_qty_1d", "id")
    .limit(5)
)
