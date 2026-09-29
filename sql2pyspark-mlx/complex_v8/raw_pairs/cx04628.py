from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

with_details = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_unit_price_1d",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

enriched = (
    with_details
    .select(
        "*",
        (F.col("sku_unit_cost") + F.col("prior_unit_price_1d")).alias("sku_unit_cost_plus_prior_unit_price_1d"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_stats
    .groupBy("sku_id")
    .pivot("sku_category", ["Apparel", "Beauty", "Toys"])
    .agg(F.min("prior_unit_price_1d"))
    .orderBy("sku_id")
)
