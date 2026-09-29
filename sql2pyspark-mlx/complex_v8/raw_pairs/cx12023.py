from pyspark.sql import functions as F

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.unit_price").alias("prior_unit_price_12h"),
    )
)

joined = (
    covered.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.array_contains(F.col("sk.labels"), "express")), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_12h",
        "s.prior_unit_price_12h",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_activity = (
    joined
    .crossJoin(joined.agg(F.min("sku_unit_cost").alias("overall_min_sku_unit_cost")))
    .filter(F.col("sku_unit_cost") > F.col("overall_min_sku_unit_cost"))
    .drop("overall_min_sku_unit_cost")
)

combined_details_sku = (
    with_activity.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") == "Apparel"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_12h",
        "s.prior_unit_price_12h",
        "s.sku_category",
        "s.sku_unit_cost",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost_2"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    combined_details_sku
    .filter(F.col("sku_category") == "Apparel")
    .select(
        "id",
        "moved_at",
        "prior_rows_12h",
        "prior_unit_price_12h",
        "sku_category",
        "sku_unit_cost",
        "sku_weight_kg",
        "sku_unit_cost_2",
        "sku_is_active",
        F.coalesce(F.col("sku_category"), F.lit("UNKNOWN")).alias("sku_category_or_unknown"),
    )
    .orderBy("prior_rows_12h", F.col("sku_unit_cost_2").asc_nulls_last())
)
