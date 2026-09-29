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

with_details = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_7d",
        "s.prior_unit_price_7d",
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_details
    .select(
        "id",
        "sku_id",
        "prior_rows_7d",
        "prior_unit_price_7d",
        "sku_labels",
    )
    .orderBy(F.col("prior_rows_7d").desc(), F.col("id").desc())
)
