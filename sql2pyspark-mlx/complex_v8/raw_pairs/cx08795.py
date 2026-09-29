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
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

with_details = (
    matched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_12h",
        "s.prior_qty_12h",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    with_details
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_12h",
        F.least(F.col("sku_weight_kg"), F.col("prior_qty_12h")).alias("least_sku_weight_kg_prior_qty_12h"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
    .orderBy(F.col("prior_rows_12h").desc(), F.col("moved_at").asc())
)
