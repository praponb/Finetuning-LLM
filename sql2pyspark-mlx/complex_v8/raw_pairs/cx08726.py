from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("qty") >= 154) & (F.col("operator").isin("op_bee", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select("s.*", F.col("sk.labels").alias("sku_labels"))
)

combined_details_warehouse = (
    with_parent.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (~F.col("w.city").isin("Singapore", "Kuala Lumpur")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.moved_at_hour",
        "s.sku_labels",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

filtered = (
    combined_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 7) & (F.col("st.qty") < 370), "left_semi")
    .filter(F.col("moved_at_hour") >= 8)
)

per_group = (
    filtered
    .groupBy(
        "moved_at_hour",
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("warehouse_city").isin("Kuala Lumpur", "Bangkok"), 1)) / F.count("*"), 1).alias("pct_warehouse_city_not_in_kuala"),
        F.sum(F.when(F.col("warehouse_city").isin("Bangkok", "Singapore"), F.col("warehouse_capacity")).otherwise(0)).alias("warehouse_capacity_warehouse_city_in_bangkok_si"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.datediff(F.max("warehouse_opened_on"), F.min("warehouse_opened_on")).alias("warehouse_opened_on_span_days"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.sum("unit_price").alias("total_unit_price"),
        F.max("days_since_moved_at").alias("max_days_since_moved_at_check"),
    )
    .filter(F.col("max_days_since_moved_at_check") > 210)
    .drop("max_days_since_moved_at_check")
)

annotated = (
    per_group
    .withColumn("days_since_moved_at_month", F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at_month")))
)

result = (
    annotated
    .filter(F.col("warehouse_capacity_warehouse_city_in_bangkok_si") > 25000)
    .select(
        "moved_at_hour",
        "warehouse_opened_on_month",
        "moved_at_month",
        "pct_warehouse_city_not_in_kuala",
        "distinct_sku_ids",
        "warehouse_opened_on_span_days",
        "distinct_warehouse_ids",
        "total_unit_price",
        "days_since_moved_at_month",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at_month")).alias("days_since_moved_at_month_2"),
        F.abs(F.col("total_unit_price") - 1650.5).alias("total_unit_price_distance"),
    )
    .orderBy(
        F.col("moved_at_month").desc(),
        F.col("distinct_sku_ids").asc_nulls_last(),
    )
)
