from pyspark.sql import functions as F

matched = (
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
    matched
    .select(
        "*",
        F.coalesce(F.col("prior_rows_7d"), F.lit(0)).alias("prior_rows_7d_filled"),
        (F.floor(F.col("prior_qty_7d") / 500) * 500).alias("prior_qty_7d_bucket"),
    )
)

result = (
    labeled
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_qty_7d",
        "prior_rows_7d_filled",
        "prior_qty_7d_bucket",
        F.round(F.col("prior_rows_7d_filled") / F.nullif(F.col("prior_rows_7d"), F.lit(0)), 3).alias("prior_rows_7d_filled_per_prior_rows_7d"),
    )
    .orderBy(F.col("moved_at").asc_nulls_last())
)
