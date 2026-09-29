from pyspark.sql import functions as F

joined_ranges = (
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

combined_details = (
    joined_ranges.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_3d",
        "s.prior_unit_price_3d",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("move_move_type_count"),
    )
)

profiled = (
    combined_details.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.total_move_unit_price",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "moved_at",
        "sku_unit_cost",
        "sku_labels",
        "sku_weight_kg",
        "total_move_unit_price",
        "move_move_type_count",
    )
    .orderBy(
        F.col("sku_weight_kg").desc(),
        F.col("move_move_type_count").asc_nulls_last(),
    )
)
