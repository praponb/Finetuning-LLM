from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_qty_tier_warehouse_id = Window.partitionBy("qty_tier", "warehouse_id")
w_qty_tier_by_qty_running = Window.partitionBy("qty_tier").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_tier_by_qty_prior_6 = Window.partitionBy("qty_tier").orderBy("qty", "id").rowsBetween(-6, -1)
w_qty_tier_by_qty = Window.partitionBy("qty_tier").orderBy("qty", "id")
w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_operator_in = Window.orderBy("distinct_operator_in_qty_tier_warehouse_id", "id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        F.when(F.col("qty") >= 372, "HIGH").when(F.col("qty") >= 189, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

with_windows = (
    cleaned_moves
    .select(
        "*",
        F.min("unit_price").over(w_qty_tier_warehouse_id).alias("qty_tier_warehouse_id_min_unit_price"),
        F.first("qty", ignorenulls=True).over(w_qty_tier_by_qty_running).alias("first_known_qty"),
        F.round(F.avg("unit_price").over(w_qty_tier_by_qty_prior_6), 2).alias("unit_price_avg_6_prior"),
        F.lead("unit_price", 2, 0).over(w_qty_tier_by_qty).alias("next2_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w_qty_tier_by_qty_running).alias("first_known_unit_price"),
        F.size(F.collect_set("operator").over(w_qty_tier_warehouse_id)).alias("distinct_operator_in_qty_tier_warehouse_id"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.min("qty").over(w_operator_sku_id).alias("operator_sku_id_min_qty"),
        F.size(F.collect_set("move_type").over(w_operator_sku_id)).alias("distinct_move_type_in_operator_sku_id"),
        F.last("moved_at").over(w_all_by_unit_price_full).alias("last_moved_at"),
        (F.col("unit_price_avg_6_prior") - F.lag("unit_price_avg_6_prior").over(w_all_by_distinct_operator_in)).alias("unit_price_avg_6_prior_change"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "OUTBOUND"), "left_anti")
    .filter(F.col("operator_ref").isNotNull())
)

result = (
    without_matches
    .filter(F.col("first_known_unit_price") < 307.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "operator_ref",
        "qty_tier",
        "operator_prefix",
        "first_known_qty",
        "unit_price_avg_6_prior",
        "first_known_unit_price",
        "distinct_operator_in_qty_tier_warehouse_id",
        "operator_sku_id_min_qty",
        "distinct_move_type_in_operator_sku_id",
        "last_moved_at",
        "unit_price_avg_6_prior_change",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
    .orderBy(F.col("qty").asc_nulls_last())
)
