from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"))
)

with_counts = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.avg_move_unit_price")
)

result = (
    with_counts
    .rollup("move_type", "warehouse_id")
    .agg(
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.max("qty").alias("max_qty"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
