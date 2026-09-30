from pyspark.sql import functions as F

valid = df_stock_moves.filter((F.col("unit_price") >= 161.4) | (F.col("move_type") == "OUTBOUND"))

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(~F.col("move_type").isin("TRANSFER", "INBOUND"), 1)).alias("move_move_type_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

with_children_moves = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

enriched = (
    with_children_moves
    .filter(F.col("move_id") >= 59)
    .select("*", (F.floor(F.col("move_qty") / 100) * 100).alias("move_qty_bucket"))
)

result = (
    enriched
    .rollup("move_warehouse_id", "move_operator", "sku_id")
    .agg(
        F.grouping("move_warehouse_id").alias("is_total_move_warehouse_id"),
        F.round(F.avg("move_qty_bucket"), 2).alias("avg_move_qty_bucket"),
    )
    .orderBy(
        F.col("move_warehouse_id").asc_nulls_last(),
        F.col("move_operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
