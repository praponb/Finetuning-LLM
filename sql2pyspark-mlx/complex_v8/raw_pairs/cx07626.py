from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_qty_tier = Window.partitionBy("sku_id", "qty_tier")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_sku_id_qty_tier = Window.partitionBy("operator").orderBy("sku_id_qty_tier_avg_qty")
w_operator_by_sku_id_qty_tier_2 = Window.partitionBy("operator").orderBy("sku_id_qty_tier_avg_qty", "id").rowsBetween(-3, Window.currentRow)
w_operator_by_sku_id_qty_tier_3 = Window.partitionBy("operator").orderBy("sku_id_qty_tier_avg_qty", "id")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id_qty_tier")

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
)

labeled = (
    filtered_moves
    .withColumn("qty_tier", F.when(F.col("qty") >= 275, "A").when(F.col("qty") >= 123, "B").otherwise("C"))
)

without_matches = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

with_parent = (
    without_matches.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_prefix",
        "s.qty_tier",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.size(F.collect_set("warehouse_city").over(w_sku_id_qty_tier)).alias("distinct_warehouse_city_in_sku_id_qty_tier"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_qty_tier)).alias("distinct_warehouse_id_in_sku_id_qty_tier"),
        F.max("unit_price").over(w_sku_id_move_type_by_moved_at).alias("max_unit_price_so_far"),
        F.round(F.avg("qty").over(w_sku_id_qty_tier), 2).alias("sku_id_qty_tier_avg_qty"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.rank().over(w_operator_by_sku_id_qty_tier).alias("sku_id_qty_tier_avg_qty_rank"),
        F.round(F.avg("sku_id_qty_tier_avg_qty").over(w_operator_by_sku_id_qty_tier_2), 2).alias("sku_id_qty_tier_avg_qty_avg_4"),
        F.lead("moved_at").over(w_operator_by_sku_id_qty_tier_3).alias("next_moved_at"),
        F.lag("sku_id_qty_tier_avg_qty").over(w_operator_by_sku_id_qty_tier_3).alias("prev_sku_id_qty_tier_avg_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_distinct_warehouse_id_in_sku), F.lit(0)), 4).alias("qty_share_of_distinct_warehouse_id_in_sku_id_qty"),
    )
)

result = (
    with_windows
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "qty_tier",
        "warehouse_wh_code",
        "warehouse_city",
        "distinct_warehouse_city_in_sku_id_qty_tier",
        "distinct_warehouse_id_in_sku_id_qty_tier",
        "sku_id_qty_tier_avg_qty",
        "sku_id_qty_tier_avg_qty_rank",
        "sku_id_qty_tier_avg_qty_avg_4",
        "next_moved_at",
        "prev_sku_id_qty_tier_avg_qty",
        "qty_share_of_distinct_warehouse_id_in_sku_id_qty",
        F.round((F.unix_timestamp(F.col("next_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 3600, 1).alias("hours_moved_at_to_next_moved_at"),
    )
)
