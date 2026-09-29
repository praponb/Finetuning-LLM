from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator").orderBy(F.col("moved_at").desc(), F.col("id").desc())
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

latest_per_key = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.last("unit_price").over(w_all_by_moved_at_full).alias("last_unit_price"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_move_type_operator), 2).alias("days_since_moved_at_vs_move_type_operator_avg"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.count(F.lit(1)).over(w_all_by_moved_at_running).alias("running_count"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_move_type_operator), F.lit(0)), 4).alias("days_since_moved_at_share_of_move_type_operator"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.when(F.col("days_since_moved_at") >= 595, "A").when(F.col("days_since_moved_at") >= 210, "B").otherwise("C").alias("days_since_moved_at_tier"),
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "operator_ref",
        "days_since_moved_at",
        "last_unit_price",
        "days_since_moved_at_vs_move_type_operator_avg",
        "unit_price_vs_move_type_operator_avg",
        "days_since_moved_at_share_of_move_type_operator",
        "days_since_moved_at_tier",
        F.when(F.col("running_count") >= 17, "large").when(F.col("running_count") >= 6, "medium").otherwise("small").alias("running_count_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at_2"),
    )
    .orderBy("id")
)
