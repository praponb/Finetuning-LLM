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

totals = (
    in_window
    .filter(F.col("sku_id") >= 16)
    .groupBy(
        "prior_rows_7d",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.percentile_approx("prior_qty_7d", 0.5).alias("median_prior_qty_7d"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    totals
    .select("prior_rows_7d", "median_prior_qty_7d")
    .orderBy("prior_rows_7d", "median_prior_qty_7d")
)
