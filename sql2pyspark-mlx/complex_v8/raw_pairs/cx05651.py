from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_by_mov_2 = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id")

filtered = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
    )
)

metrics = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        F.nth_value("moved_at", 2).over(w_all_by_moved_at_full).alias("second_moved_at"),
        F.first("moved_at", ignorenulls=True).over(w_all_by_moved_at_running).alias("first_known_moved_at"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_operator_by_mov).alias("second_moved_at_2"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_operator_by_mov_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

per_group = (
    metrics
    .filter(F.col("moved_at") >= F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .groupBy(
        F.date_trunc("month", "second_moved_at").alias("second_moved_at_month"),
        "sku_id",
    )
    .agg(
        F.min("minutes_since_prev_moved_at").alias("min_minutes_since_prev_moved_at"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("minutes_since_prev_moved_at"), 2).alias("stddev_minutes_since_prev_moved_at"),
        F.min("first_known_moved_at").alias("first_first_known_moved_at"),
        F.max("minutes_since_prev_moved_at").alias("max_minutes_since_prev_moved_at_check"),
    )
    .filter(F.col("max_minutes_since_prev_moved_at_check") > 1500.0)
    .drop("max_minutes_since_prev_moved_at_check")
)

combined_details_sku = (
    per_group.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.length(F.trim(F.col("sk.sku_code"))) >= 7), "inner")
    .filter(F.col("sk.weight_kg") > 6.4)
    .select(
        "s.second_moved_at_month",
        "s.sku_id",
        "s.min_minutes_since_prev_moved_at",
        "s.stddev_minutes_since_prev_moved_at",
        "s.first_first_known_moved_at",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

result = (
    combined_details_sku
    .select(
        "second_moved_at_month",
        "sku_id",
        "stddev_minutes_since_prev_moved_at",
        "first_first_known_moved_at",
        "sku_weight_kg",
        "sku_labels",
        "sku_is_active",
        F.date_trunc("week", F.col("second_moved_at_month")).alias("second_moved_at_month_week"),
        F.coalesce(F.col("sku_weight_kg"), F.lit(0.0)).alias("sku_weight_kg_filled"),
    )
    .orderBy(F.col("sku_id").desc())
)
