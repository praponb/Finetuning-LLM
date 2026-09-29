from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

warehouse_id_stats = (
    scoped_moves
    .groupBy("warehouse_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

above_average = (
    scoped_moves.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price") == F.col("g.max_unit_price"))
    .select(
        "s.*",
        "g.max_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.max_unit_price"), 2).alias("unit_price_minus_max"),
    )
)

combined_details = (
    above_average.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.max_unit_price",
        "s.unit_price_minus_max",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

move_type_stats = (
    combined_details
    .groupBy("move_type")
    .agg(F.avg("unit_price_minus_max").alias("avg_unit_price_minus_max"))
)

outliers = (
    combined_details.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price_minus_max") > F.col("g.avg_unit_price_minus_max"))
    .select(
        "s.*",
        "g.avg_unit_price_minus_max",
        F.round(F.col("s.unit_price_minus_max") - F.col("g.avg_unit_price_minus_max"), 2).alias("unit_price_minus_max_minus_avg"),
    )
)

result = (
    outliers
    .rollup("move_type", "operator", "sku_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("n_operator_is_not_null"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
