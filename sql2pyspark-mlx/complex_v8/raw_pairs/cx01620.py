from pyspark.sql import functions as F

in_window = (
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

joined_sku = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_12h",
        "s.prior_unit_price_12h",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
    )
)

without_matches = (
    joined_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_dew", "op_bee")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "sku_id",
        "prior_rows_12h",
        "prior_unit_price_12h",
        "sku_is_active",
        F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_unit_price_12h"), F.lit(0)), 3).alias("prior_rows_12h_per_prior_unit_price_12h"),
        (F.col("prior_rows_12h") + F.col("prior_unit_price_12h")).alias("prior_rows_12h_plus_prior_unit_price_12h"),
    )
    .orderBy(F.col("prior_rows_12h_per_prior_unit_price_12h").desc())
)
