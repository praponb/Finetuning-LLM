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
        F.sum("b.unit_price").alias("prior_unit_price_7d"),
    )
)

grouped = (
    in_window
    .groupBy(F.year("moved_at").alias("moved_at_year"))
    .agg(
        F.round(F.avg("prior_unit_price_7d"), 2).alias("avg_prior_unit_price_7d"),
        F.count("*").alias("row_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
    .filter(F.col("row_count") >= 2)
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.max("avg_prior_unit_price_7d").alias("overall_max_avg_prior_unit_price_7d")))
    .filter(F.col("avg_prior_unit_price_7d") < F.col("overall_max_avg_prior_unit_price_7d"))
    .drop("overall_max_avg_prior_unit_price_7d")
)

with_activity = (
    screened
    .crossJoin(screened.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    with_activity
    .select(
        "moved_at_year",
        "avg_prior_unit_price_7d",
        "row_count",
        "first_moved_at",
        F.date_trunc("hour", F.col("first_moved_at")).alias("first_moved_at_hour"),
    )
)
