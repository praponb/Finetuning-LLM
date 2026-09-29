from pyspark.sql import functions as F

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

sku_id_stats = (
    covered
    .groupBy("sku_id")
    .agg(F.percentile_approx("prior_unit_price_3d", 0.5).alias("median_prior_unit_price_3d"))
)

vs_group = (
    covered.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.prior_unit_price_3d") >= F.col("g.median_prior_unit_price_3d"))
    .select(
        "s.*",
        "g.median_prior_unit_price_3d",
        F.round(F.col("s.prior_unit_price_3d") - F.col("g.median_prior_unit_price_3d"), 2).alias("prior_unit_price_3d_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_3d",
        "prior_unit_price_3d",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)
