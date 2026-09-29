from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
)

filtered = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("A%")) & (~F.array_contains(F.col("sk.labels"), "express")), "left_semi")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

grouped = (
    without_matches
    .groupBy(
        F.when(F.col("unit_price") >= 323.9, "HIGH").when(F.col("unit_price") >= 160.8, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        "operator",
        "sku_id",
    )
    .agg(
        F.count(F.when(F.col("operator").isin("op_ann", "op_bee", "op_chan"), 1)).alias("n_operator_in_op_ann_op_bee_op"),
        F.sum(F.when(F.col("operator").isin("op_chan", "op_dew", "op_bee"), 1).otherwise(0)).alias("n_operator_in_op_chan_op_dew_o"),
    )
)

result = (
    grouped
    .filter(F.col("operator") == "op_bee")
    .select(
        "unit_price_tier",
        "operator",
        "n_operator_in_op_ann_op_bee_op",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.when(F.col("unit_price_tier").isin("LOW", "MEDIUM"), "group_a").when(F.col("unit_price_tier") == "HIGH", "group_b").otherwise("other").alias("unit_price_tier_group"),
    )
    .orderBy(F.col("operator").desc())
)
