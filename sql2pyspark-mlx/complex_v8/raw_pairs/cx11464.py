from pyspark.sql import functions as F

joined_ranges = (
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

qualified = (
    joined_ranges.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.array_contains(F.col("sk.labels"), "bulky")) & (F.col("sk.unit_cost") >= 158.7), "left_semi")
)

combined_details_sku = (
    qualified.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_12h",
        "s.prior_unit_price_12h",
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    combined_details_sku
    .select("id", "prior_unit_price_12h", "sku_weight_kg")
    .unpivot(
        ids=["id"],
        values=["prior_unit_price_12h", "sku_weight_kg"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
