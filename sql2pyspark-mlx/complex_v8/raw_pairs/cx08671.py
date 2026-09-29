from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("qty").between(99, 348))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

scored = prepared_moves.withColumn("moved_at_date_week", F.date_trunc("week", F.col("moved_at_date")))

grouped = (
    scored
    .groupBy(
        "move_type",
        F.date_trunc("month", "moved_at_date").alias("moved_at_date_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.sum("qty").alias("total_qty"),
        F.max("unit_price").alias("max_unit_price"),
        F.sum(F.when(F.col("move_type").isin("OUTBOUND", "ADJUST", "TRANSFER"), 1).otherwise(0)).alias("n_move_type_in_outbound_adjust"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("unit_price").alias("avg_unit_price_check"),
    )
    .filter(F.col("avg_unit_price_check") > 160.8)
    .drop("avg_unit_price_check")
)

result = (
    grouped
    .select(
        "move_type",
        "moved_at_date_month",
        "distinct_sku_ids",
        "n_move_type_in_outbound_adjust",
        "distinct_ids",
    )
    .orderBy(F.col("moved_at_date_month").desc())
)
