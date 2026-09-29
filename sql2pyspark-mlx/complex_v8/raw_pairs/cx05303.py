from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-13 10:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

labeled = (
    filtered_moves
    .withColumn("operator_slug", F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")))
)

enriched = labeled.withColumn("least_unit_price_qty", F.least(F.col("unit_price"), F.col("qty")))

filtered = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on") >= F.lit("2024-11-25").cast("date")) & (F.col("w.wh_code").isNotNull()), "left_anti")
)

result = (
    filtered
    .cube("operator", "move_type")
    .agg(F.sum("unit_price_per_qty").alias("total_unit_price_per_qty"))
    .orderBy(F.col("operator").asc_nulls_last(), F.col("move_type").asc_nulls_last())
)
