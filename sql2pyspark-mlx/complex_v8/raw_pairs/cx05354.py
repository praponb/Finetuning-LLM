from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_warehouse_id_running))

combined_details_sku = (
    sessionized.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        "s.new_session_flag",
        "s.burst_id",
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

per_group = (
    combined_details_sku
    .groupBy("new_session_flag", "sku_is_active")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.percentile_approx("unit_price_filled", 0.5).alias("median_unit_price_filled"),
        F.round(F.avg("sku_weight_kg"), 2).alias("avg_sku_weight_kg"),
        F.round(F.stddev("sku_weight_kg"), 2).alias("stddev_sku_weight_kg"),
    )
)

result = (
    per_group
    .filter(F.col("stddev_sku_weight_kg") > 10.0)
    .select(
        "new_session_flag",
        "sku_is_active",
        "move_type_list",
        "median_unit_price_filled",
        "avg_sku_weight_kg",
        F.round(F.col("stddev_sku_weight_kg") * 1.1, 2).alias("stddev_sku_weight_kg_adj"),
        F.when(F.col("sku_is_active"), 1).otherwise(0).alias("sku_is_active_flag"),
    )
    .orderBy("median_unit_price_filled", F.col("stddev_sku_weight_kg_adj").desc())
)
