from pyspark.sql import functions as F

covered = (
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

combined_details_sku = (
    covered.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.array_contains(F.col("sk.labels"), "express")), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_7d",
        "s.prior_unit_price_7d",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "prior_rows_7d",
        "prior_unit_price_7d",
        "sku_labels",
        "sku_category",
        F.when(F.col("sku_is_active"), 1).otherwise(0).alias("sku_is_active_flag"),
        F.when(F.col("prior_rows_7d") >= 5, "A").when(F.col("prior_rows_7d") >= 3, "B").otherwise("C").alias("prior_rows_7d_tier"),
    )
    .orderBy("prior_unit_price_7d", F.col("sku_id").desc())
)
