from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

with_details = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.capacity") < 11506)
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
        "s.move_type_len",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

enriched_warehouse = (
    with_details.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity_2"),
    )
)

joined_warehouse = (
    enriched_warehouse.alias("s")
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
        "s.unit_price_minus_qty",
        "s.move_type_len",
        "s.warehouse_capacity",
        "s.warehouse_city",
        "s.warehouse_opened_on",
        "s.warehouse_wh_code",
        "s.warehouse_capacity_2",
        F.col("w.city").alias("warehouse_city_2"),
    )
)

screened = (
    joined_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-18").cast("date")) & (F.col("st.qty") >= 168), "left_anti")
    .filter(F.col("move_type_len") < 11)
)

filtered = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.length(F.trim(F.col("w.wh_code"))) >= 6) & (F.year(F.col("w.opened_on")) == 2024), "left_anti")
)

result = (
    filtered
    .groupingSets(
        [["operator", "warehouse_city"], ["operator"]],
        "operator", "warehouse_city",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("warehouse_opened_on").alias("first_warehouse_opened_on"),
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
        F.min("move_type_len").alias("min_move_type_len"),
    )
)
