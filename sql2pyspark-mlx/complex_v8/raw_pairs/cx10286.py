from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

with_parent_sku = (
    valid.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

without_matches = (
    with_parent_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") != "op_dew") & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

filtered = (
    without_matches.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") <= 11506) & (F.year(F.col("w.opened_on")) == 2025), "left_anti")
)

stats = (
    filtered
    .groupBy(
        F.year("moved_at").alias("moved_at_year"),
        F.when(F.col("sku_unit_cost") >= 320.4, "large").when(F.col("sku_unit_cost") >= 164.5, "medium").otherwise("small").alias("sku_unit_cost_tier"),
        "operator",
    )
    .agg(
        F.datediff(F.max("warehouse_opened_on"), F.min("warehouse_opened_on")).alias("warehouse_opened_on_span_days"),
        F.count("*").alias("cnt"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.round(F.sum("qty") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_to_unit_price_ratio"),
    )
)

filtered_2 = (
    stats
    .crossJoin(stats.agg(F.max("warehouse_opened_on_span_days").alias("overall_max_warehouse_opened_on_span_days")))
    .filter(F.col("warehouse_opened_on_span_days") < F.col("overall_max_warehouse_opened_on_span_days"))
    .drop("overall_max_warehouse_opened_on_span_days")
)

result = (
    filtered_2
    .groupBy("sku_unit_cost_tier", "cnt")
    .pivot("operator", ["op_eve", "op_chan", "op_bee", "op_dew"])
    .agg(F.count("warehouse_opened_on_span_days"))
    .orderBy("sku_unit_cost_tier", "cnt")
)
