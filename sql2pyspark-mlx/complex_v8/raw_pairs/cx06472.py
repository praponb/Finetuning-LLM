from pyspark.sql import functions as F

joined_ranges = (
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
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

stats = (
    joined_ranges
    .groupBy("prior_rows_12h", "sku_id", F.year("moved_at").alias("moved_at_year"))
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.min("prior_rows_12h").alias("min_prior_rows_12h"),
        F.percentile_approx("prior_qty_12h", 0.5).alias("median_prior_qty_12h"),
        F.max("moved_at").alias("last_moved_at"),
        F.avg("prior_rows_12h").alias("avg_prior_rows_12h_check"),
    )
    .filter(F.col("avg_prior_rows_12h_check") > 3)
    .drop("avg_prior_rows_12h_check")
)

result = (
    stats
    .select(
        "prior_rows_12h",
        "sku_id",
        "min_prior_rows_12h",
        "moved_at_year",
        "median_prior_qty_12h",
    )
    .unpivot(
        ids=["prior_rows_12h", "sku_id"],
        values=["min_prior_rows_12h", "moved_at_year", "median_prior_qty_12h"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
