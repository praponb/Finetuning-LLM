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

per_group = (
    covered
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"), "sku_id")
    .agg(
        F.min("prior_unit_price_1d").alias("min_prior_unit_price_1d"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("prior_unit_price_1d").alias("max_prior_unit_price_1d_check"),
    )
    .filter(F.col("max_prior_unit_price_1d_check") > 500.0)
    .drop("max_prior_unit_price_1d_check")
)

result = (
    per_group
    .select("moved_at_month", "min_prior_unit_price_1d", "last_moved_at")
    .orderBy(
        F.col("last_moved_at").asc_nulls_last(),
        F.col("moved_at_month").asc_nulls_last(),
    )
)
