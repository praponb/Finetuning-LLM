from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_bee", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_details_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.unit_price_plus_qty",
        "s.operator_move_type_label",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("qty").alias("min_move_qty"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

with_stats = (
    with_details_warehouse.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.min_move_qty",
        "st.last_moved_at_2",
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity").between(4912, 11506)), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.unit_price_plus_qty",
        "s.operator_move_type_label",
        "s.warehouse_wh_code",
        "s.warehouse_opened_on",
        "s.sku_unit_cost",
        "s.last_moved_at",
        "s.min_move_qty",
        "s.last_moved_at_2",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("w.city").alias("warehouse_city"),
    )
)

summary = (
    with_parent
    .groupBy(
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        "warehouse_city",
    )
    .agg(
        F.min("warehouse_opened_on").alias("first_warehouse_opened_on"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
    )
)

enriched = (
    summary
    .select(
        "*",
        F.substring(F.trim(F.col("warehouse_city")), 1, 2).alias("warehouse_city_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_city")), " ", "_")).alias("warehouse_city_slug"),
    )
)

enriched_2 = (
    enriched
    .select(
        "warehouse_opened_on_month",
        "warehouse_city",
        "first_warehouse_opened_on",
        "distinct_ids",
        "warehouse_city_list",
        "warehouse_city_prefix",
        F.abs(F.col("avg_qty") - 225.6).alias("avg_qty_distance"),
    )
)

result = (
    enriched_2
    .filter(F.size(F.col("warehouse_city_list")) >= 1)
    .select(
        "warehouse_opened_on_month",
        "warehouse_city",
        "first_warehouse_opened_on",
        "distinct_ids",
        "warehouse_city_list",
        "warehouse_city_prefix",
        "avg_qty_distance",
        F.substring(F.trim(F.col("warehouse_city_prefix")), 1, 4).alias("warehouse_city_prefix_prefix"),
    )
    .orderBy(
        F.col("warehouse_city_prefix_prefix").asc(),
        F.col("warehouse_opened_on_month").asc_nulls_last(),
    )
)
