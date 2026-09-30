from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_distinct_warehouse_id = Window.partitionBy("sku_id", "distinct_warehouse_ids").orderBy("unit_price_per_qty_upper_trim_warehouse_wh_code", "move_type", "sku_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_all_by_distinct_warehouse_id = Window.orderBy("distinct_warehouse_ids", "move_type", "sku_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy("unit_price_per_qty_upper_trim_warehouse_wh_code", "move_type", "sku_id").rowsBetween(-6, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("qty").between(145, 355))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.year(F.col("moved_at")).alias("moved_at_year"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.capacity").between(4912, 11506))
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

enriched_sku = (
    combined_details.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.size(F.col("sk.labels")) >= 1)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_minus_qty",
        "s.unit_price_per_qty",
        "s.warehouse_wh_code",
        "s.warehouse_capacity",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

per_group = (
    enriched_sku
    .groupBy("move_type", "sku_id")
    .agg(
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("warehouse_wh_code"))).like("B%"), F.col("unit_price_per_qty")).otherwise(0)).alias("unit_price_per_qty_upper_trim_warehouse_wh_code"),
        F.max("unit_price_per_qty").alias("max_unit_price_per_qty_check"),
    )
    .filter(F.col("max_unit_price_per_qty_check") > 3.0)
    .drop("max_unit_price_per_qty_check")
)

with_trends = (
    per_group
    .select(
        "*",
        F.last("distinct_warehouse_ids").over(w_sku_id_distinct_warehouse_id).alias("last_distinct_warehouse_ids"),
        F.round(F.col("distinct_warehouse_ids") - F.avg("distinct_warehouse_ids").over(w_move_type), 2).alias("distinct_warehouse_ids_vs_move_type_avg"),
        F.first("distinct_warehouse_ids", ignorenulls=True).over(w_all_by_distinct_warehouse_id).alias("first_known_distinct_warehouse_ids"),
        F.round(F.avg("distinct_warehouse_ids").over(w_move_type_sku_id_by_unit_pri), 2).alias("distinct_warehouse_ids_avg_7"),
    )
)

with_children_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.move_type",
        "s.sku_id",
        "s.distinct_warehouse_ids",
        "s.distinct_warehouse_ids_vs_move_type_avg",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

without_matches = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") <= 3), "left_anti")
)

result = (
    without_matches
    .select(
        "move_type",
        "sku_id",
        "distinct_warehouse_ids",
        "distinct_warehouse_ids_vs_move_type_avg",
        "move_id",
        "move_qty",
        "move_operator",
        F.date_trunc("hour", F.col("move_moved_at")).alias("move_moved_at_hour"),
        F.date_format(F.col("move_moved_at"), "MMM yyyy").alias("move_moved_at_label"),
    )
    .orderBy(F.col("move_id").desc(), "move_type", "sku_id")
    .limit(10)
)
