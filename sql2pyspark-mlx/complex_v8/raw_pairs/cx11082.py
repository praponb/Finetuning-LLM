from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("unit_price") >= 86.3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

move_type_or_unknown_stats = (
    prepared
    .groupBy("move_type_or_unknown")
    .agg(F.percentile_approx("moved_at_dayofweek", 0.5).alias("median_moved_at_dayofweek"))
)

above_average = (
    prepared.alias("s")
    .join(move_type_or_unknown_stats.alias("g"), F.col("g.move_type_or_unknown") == F.col("s.move_type_or_unknown"), "inner")
    .filter(F.col("s.moved_at_dayofweek") >= F.col("g.median_moved_at_dayofweek"))
    .select(
        "s.*",
        "g.median_moved_at_dayofweek",
        F.round(F.col("s.moved_at_dayofweek") - F.col("g.median_moved_at_dayofweek"), 2).alias("moved_at_dayofweek_minus_median"),
    )
)

joined = (
    above_average.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.unit_cost").between(134.3, 302.5))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.move_type_or_unknown",
        "s.unit_price_plus_qty",
        "s.moved_at_dayofweek",
        "s.median_moved_at_dayofweek",
        "s.moved_at_dayofweek_minus_median",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at_dayofweek",
        "median_moved_at_dayofweek",
        "moved_at_dayofweek_minus_median",
        "sku_is_active",
        "sku_unit_cost",
        F.when(F.col("unit_price_plus_qty") >= 800.2, "A").when(F.col("unit_price_plus_qty") >= 400.6, "B").otherwise("C").alias("unit_price_plus_qty_tier"),
        F.round(F.col("unit_price_plus_qty") / F.nullif(F.col("sku_unit_cost"), F.lit(0)), 3).alias("unit_price_plus_qty_per_sku_unit_cost"),
    )
    .orderBy(F.col("moved_at_dayofweek_minus_median").desc())
)
