from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at_runnin = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.abs(F.col("qty") - 148).alias("qty_distance"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

annotated = (
    recent_moves
    .filter(F.col("operator").isNotNull())
    .select("*", F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"))
)

ranked = (
    annotated
    .withColumn("running_qty", F.sum("qty").over(w_move_type_by_moved_at_runnin))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w_move_type_by_moved_at_runnin))
)

joined_warehouse = (
    ranked.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.qty_bucket",
        "s.qty_distance",
        "s.operator_or_unknown",
        "s.moved_at_year",
        "s.last_known_unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

operator_or_unknown_stats = (
    joined_warehouse
    .groupBy("operator_or_unknown")
    .agg(F.percentile_approx("last_known_unit_price", 0.5).alias("median_last_known_unit_price"))
)

compared = (
    joined_warehouse.alias("s")
    .join(operator_or_unknown_stats.alias("g"), F.col("g.operator_or_unknown") == F.col("s.operator_or_unknown"), "inner")
    .filter(F.col("s.last_known_unit_price") >= F.col("g.median_last_known_unit_price"))
    .select(
        "s.*",
        "g.median_last_known_unit_price",
        F.round(F.col("s.last_known_unit_price") - F.col("g.median_last_known_unit_price"), 2).alias("last_known_unit_price_minus_median"),
    )
)

result = (
    compared
    .filter(F.col("last_known_unit_price") > 111.7)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "qty_bucket",
        "qty_distance",
        "operator_or_unknown",
        "warehouse_opened_on",
        "warehouse_capacity",
        "median_last_known_unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("warehouse_opened_on")).alias("days_since_warehouse_opened_on"),
        F.lower(F.trim(F.col("operator_or_unknown"))).alias("operator_or_unknown_clean"),
    )
    .orderBy("qty_distance", "qty_bucket")
)
