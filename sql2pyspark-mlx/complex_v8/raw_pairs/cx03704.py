from pyspark.sql import functions as F

matched = (
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
        F.sum("b.unit_price").alias("prior_unit_price_12h"),
    )
)

screened = (
    matched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date"))
)

result = (
    screened
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_12h",
        "prior_unit_price_12h",
    )
    .orderBy(F.col("id").desc(), F.col("prior_unit_price_12h").asc_nulls_last())
)
