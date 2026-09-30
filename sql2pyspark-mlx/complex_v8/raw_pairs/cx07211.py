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

joined_sku = (
    covered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_1d",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    joined_sku
    .select(
        "id",
        "moved_at",
        "prior_rows_1d",
        "sku_unit_cost",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
    .orderBy(F.col("moved_at").asc())
)
