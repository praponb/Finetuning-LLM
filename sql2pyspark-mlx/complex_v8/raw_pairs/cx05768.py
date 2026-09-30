from pyspark.sql import functions as F

covered = (
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

joined_sku = (
    covered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_1d",
        "s.prior_qty_1d",
        F.col("sk.category").alias("sku_category"),
    )
)

sku_category_stats = (
    joined_sku
    .groupBy("sku_category")
    .agg(F.max("prior_qty_1d").alias("max_prior_qty_1d"))
)

outliers = (
    joined_sku.alias("s")
    .join(sku_category_stats.alias("g"), F.col("g.sku_category") == F.col("s.sku_category"), "inner")
    .filter(F.col("s.prior_qty_1d") == F.col("g.max_prior_qty_1d"))
    .select(
        "s.*",
        "g.max_prior_qty_1d",
        F.round(F.col("s.prior_qty_1d") - F.col("g.max_prior_qty_1d"), 2).alias("prior_qty_1d_minus_max"),
    )
)

result = (
    outliers
    .filter(F.col("prior_qty_1d").between(375, 2125))
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_1d",
        "prior_qty_1d",
        "sku_category",
        "max_prior_qty_1d",
        "prior_qty_1d_minus_max",
        F.substring(F.trim(F.col("sku_category")), 1, 4).alias("sku_category_prefix"),
    )
    .orderBy("max_prior_qty_1d", "moved_at")
)
