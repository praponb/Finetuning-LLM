from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("qty") >= 198) & (F.col("unit_price") >= 160.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
    )
)

enriched = (
    recent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "unit_price_adj",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.when(F.col("operator").isin("op_ann", "op_dew"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
)

warehouse_id_stats = (
    enriched
    .groupBy("warehouse_id")
    .agg(F.avg("unit_price_adj").alias("avg_unit_price_adj"))
)

outliers = (
    enriched.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price_adj") > F.col("g.avg_unit_price_adj"))
    .select(
        "s.*",
        "g.avg_unit_price_adj",
        F.round(F.col("s.unit_price_adj") - F.col("g.avg_unit_price_adj"), 2).alias("unit_price_adj_minus_avg"),
    )
)

result = (
    outliers
    .cube("sku_id", "warehouse_id", "operator_group")
    .agg(
        F.round(F.stddev("unit_price_adj_minus_avg"), 2).alias("stddev_unit_price_adj_minus_avg"),
        F.countDistinct("operator_group").alias("distinct_operator_groups"),
    )
    .select(
        "sku_id",
        "warehouse_id",
        F.coalesce(F.col("operator_group"), F.lit("ALL")).alias("operator_group"),
        "stddev_unit_price_adj_minus_avg",
        "distinct_operator_groups",
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("operator_group").asc_nulls_last(),
    )
)
