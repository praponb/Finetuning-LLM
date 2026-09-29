from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.when(F.col("qty") >= 372, "HIGH").when(F.col("qty") >= 198, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

enriched = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (~F.col("w.city").isin("Bangkok", "Kuala Lumpur")), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.date_trunc("quarter", F.col("warehouse_opened_on")).alias("warehouse_opened_on_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

result = (
    tagged
    .groupingSets(
        [["move_type", "operator"], ["move_type"], []],
        "move_type", "operator",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("move_type").isin("OUTBOUND", "ADJUST", "TRANSFER"), 1)).alias("n_move_type_in_outbound_adjust"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("operator").asc_nulls_last())
)
