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
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

profiled = (
    in_window.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        "st.first_moved_at",
    )
)

screened = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_unit_price_3d",
        "move_count_2",
        "first_moved_at",
        F.round(F.col("prior_unit_price_3d") * 1.5, 2).alias("prior_unit_price_3d_adj"),
        F.round(F.col("prior_unit_price_3d") * 1.1, 2).alias("prior_unit_price_3d_adj_2"),
    )
    .orderBy("move_count_2")
)
