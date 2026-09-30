from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.qty").alias("prior_qty_7d"),
    )
)

labeled = (
    in_window
    .filter(F.col("sku_id").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("prior_rows_7d"), F.lit(0)).alias("prior_rows_7d_filled"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

with_activity = (
    labeled
    .crossJoin(labeled.agg(F.min("prior_rows_7d_filled").alias("overall_min_prior_rows_7d_filled")))
    .filter(F.col("prior_rows_7d_filled") > F.col("overall_min_prior_rows_7d_filled"))
    .drop("overall_min_prior_rows_7d_filled")
)

scored = (
    with_activity
    .filter(F.col("moved_at") < F.lit("2025-02-18 14:00:00").cast("timestamp"))
    .select(
        "*",
        F.least(F.col("prior_rows_7d_filled"), F.col("prior_qty_7d")).alias("least_prior_rows_7d_filled_prior_qty_7d"),
        F.coalesce(F.col("prior_qty_7d"), F.lit(0)).alias("prior_qty_7d_filled"),
    )
)

result = (
    scored
    .rollup("prior_rows_7d_filled", "sku_id")
    .agg(
        F.grouping("prior_rows_7d_filled").alias("is_total_prior_rows_7d_filled"),
        F.sum("prior_qty_7d_filled").alias("total_prior_qty_7d_filled"),
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
    )
    .orderBy(
        F.col("prior_rows_7d_filled").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
