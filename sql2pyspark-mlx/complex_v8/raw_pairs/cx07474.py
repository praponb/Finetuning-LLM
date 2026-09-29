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
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

summary = (
    covered
    .groupBy("prior_rows_1d", "sku_id")
    .agg(
        F.sum("prior_unit_price_1d").alias("total_prior_unit_price_1d"),
        F.round(F.sum("prior_unit_price_1d") / F.nullif(F.sum("prior_rows_1d"), F.lit(0)), 3).alias("prior_unit_price_1d_to_prior_rows_1d_ratio"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_stats = (
    summary.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    with_stats
    .select(
        "prior_rows_1d",
        "sku_id",
        "prior_unit_price_1d_to_prior_rows_1d_ratio",
        "move_count",
        "move_count_2",
        F.greatest(F.col("prior_rows_1d"), F.col("move_count_2")).alias("greatest_prior_rows_1d_move_count_2"),
        F.coalesce(F.col("move_count_2"), F.lit(0)).alias("move_count_2_filled"),
    )
)
