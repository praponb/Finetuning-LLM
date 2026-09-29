from pyspark.sql import functions as F

in_window = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.qty").alias("prior_qty_3d"),
    )
)

combined_details = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.category").alias("sku_category"))
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

profiled = (
    combined_details.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at")
)

result = (
    profiled
    .filter(F.hour(F.col("moved_at")).between(10, 15))
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_3d",
        "prior_qty_3d",
        "sku_category",
        "first_moved_at",
    )
    .orderBy(F.col("sku_id").desc(), "id")
    .limit(10)
)
