from pyspark.sql import functions as F

matched = (
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

scored = (
    matched
    .withColumn("prior_unit_price_1d_plus_prior_rows_1d", F.col("prior_unit_price_1d") + F.col("prior_rows_1d"))
)

result = (
    scored
    .select(
        "id",
        "moved_at",
        "prior_unit_price_1d",
        "prior_unit_price_1d_plus_prior_rows_1d",
    )
    .orderBy(F.col("prior_unit_price_1d_plus_prior_rows_1d").asc_nulls_last())
)
