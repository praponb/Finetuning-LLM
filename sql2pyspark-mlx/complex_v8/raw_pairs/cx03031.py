from pyspark.sql import functions as F

joined_ranges = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.first_moved_at",
    )
)

scored = (
    summarized_rows
    .select(
        "id",
        "moved_at",
        "prior_rows_3d",
        "prior_unit_price_3d",
        "first_moved_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_moved_at")).alias("days_since_first_moved_at"),
    )
)

with_activity = (
    scored
    .crossJoin(scored.agg(F.min("prior_rows_3d").alias("overall_min_prior_rows_3d")))
    .filter(F.col("prior_rows_3d") > F.col("overall_min_prior_rows_3d"))
    .drop("overall_min_prior_rows_3d")
)

result = (
    with_activity
    .select("id", "prior_rows_3d", "days_since_first_moved_at")
    .orderBy("id", F.col("prior_rows_3d").desc())
)
