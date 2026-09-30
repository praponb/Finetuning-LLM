from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.col("qty") >= 169) & (F.col("move_type") != "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("qty").alias("min_move_qty"),
        F.count(F.when(F.col("operator") != "op_eve", 1)).alias("move_operator_count"),
        F.max("qty").alias("max_move_qty"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.max_move_qty",
    )
)

grouped = (
    summarized_rows
    .groupBy("move_type", "warehouse_id", "sku_id")
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.sum(F.when(F.col("move_type").isNotNull(), F.col("max_move_qty")).otherwise(0)).alias("max_move_qty_move_type_is_not_null"),
        F.sum(F.when(F.col("move_type").isin("INBOUND", "TRANSFER", "OUTBOUND"), F.col("unit_price_adj"))).alias("unit_price_adj_move_type_in_inbound_transfe"),
        F.sum("qty").alias("total_qty"),
        F.sum("min_move_qty").alias("total_min_move_qty"),
        F.min("moved_at_plus_15m").alias("first_moved_at_plus_15m"),
        F.sum("unit_price_adj").alias("sum_unit_price_adj_check"),
    )
    .filter(F.col("sum_unit_price_adj_check") > 225.7)
    .drop("sum_unit_price_adj_check")
)

result = (
    grouped
    .select(
        "move_type",
        "warehouse_id",
        "sku_id",
        "distinct_operators",
        "unit_price_adj_move_type_in_inbound_transfe",
        "total_min_move_qty",
        "first_moved_at_plus_15m",
    )
    .orderBy(
        F.col("warehouse_id").desc_nulls_first(),
        F.col("total_min_move_qty").desc(),
    )
)
