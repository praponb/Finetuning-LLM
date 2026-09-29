from pyspark.sql import functions as F

matched = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.unit_price").alias("prior_unit_price_7d"),
    )
)

with_parent_sku = (
    matched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_7d",
        "s.prior_unit_price_7d",
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
    )
)

sku_id_stats = (
    with_parent_sku
    .groupBy("sku_id")
    .agg(F.max("prior_unit_price_7d").alias("max_prior_unit_price_7d"))
)

compared = (
    with_parent_sku.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.prior_unit_price_7d") == F.col("g.max_prior_unit_price_7d"))
    .select(
        "s.*",
        "g.max_prior_unit_price_7d",
        F.round(F.col("s.prior_unit_price_7d") - F.col("g.max_prior_unit_price_7d"), 2).alias("prior_unit_price_7d_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_7d",
        "prior_unit_price_7d",
        "sku_code",
        "sku_category",
        "max_prior_unit_price_7d",
        "prior_unit_price_7d_minus_max",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("sku_id").asc())
)
