from pyspark.sql import functions as F

covered = (
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

without_matches = (
    covered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") >= 90), "left_anti")
    .filter(F.col("prior_qty_7d").between(375, 1875))
)

result = (
    without_matches
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_qty_7d",
    )
    .orderBy("id", F.col("moved_at").desc())
)
