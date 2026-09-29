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

screened = (
    in_window
    .crossJoin(in_window.agg(F.avg("prior_rows_1d").alias("overall_avg_prior_rows_1d")))
    .filter(F.col("prior_rows_1d") > F.col("overall_avg_prior_rows_1d"))
    .drop("overall_avg_prior_rows_1d")
)

result = (
    screened
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_1d",
        "prior_unit_price_1d",
        F.round(F.col("prior_unit_price_1d") * 1.07, 2).alias("prior_unit_price_1d_adj"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)
