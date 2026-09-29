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

annotated = (
    matched
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at_2"),
    )
)

result = (
    annotated
    .filter(F.col("moved_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "moved_at",
        "prior_rows_7d",
        "days_since_moved_at_2",
        F.coalesce(F.col("prior_rows_7d"), F.lit(0)).alias("prior_rows_7d_filled"),
        (F.col("days_since_moved_at") + F.col("prior_qty_7d")).alias("days_since_moved_at_plus_prior_qty_7d"),
    )
    .orderBy(F.col("prior_rows_7d_filled").asc())
)
