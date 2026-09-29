from pyspark.sql import functions as F

recent_warehouses = (
    df_warehouses
    .filter((F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date"))) | (F.col("wh_code").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("city").isin("Bangkok", "Singapore"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
    )
)

line_level = (
    recent_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.city_group",
        "s.wh_code_slug",
        "s.opened_on_label",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

joined = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("st.warehouse_id"), "left")
    .select(
        "s.id",
        "s.city",
        "s.city_group",
        "s.wh_code_slug",
        "s.opened_on_label",
        "s.move_id",
        "s.move_moved_at",
        "s.move_unit_price",
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_activity = (
    joined
    .crossJoin(joined.agg(F.min("move_unit_price").alias("overall_min_move_unit_price")))
    .filter(F.col("move_unit_price") > F.col("overall_min_move_unit_price"))
    .drop("overall_min_move_unit_price")
)

scored = (
    with_activity
    .filter(F.col("warehouse_capacity") < 11768)
    .select(
        "id",
        "city",
        "wh_code_slug",
        "move_id",
        "move_moved_at",
        "move_unit_price",
        "move_operator",
        "move_warehouse_id",
        "warehouse_wh_code",
        "warehouse_capacity",
        F.date_format(F.col("move_moved_at"), "yyyy-MM-dd HH:00").alias("move_moved_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at"),
        F.when(F.col("city").isin("Kuala Lumpur", "Singapore"), "group_a").when(F.col("city") == "Bangkok", "group_b").otherwise("other").alias("city_group_2"),
    )
)

result = (
    scored
    .groupBy("move_id")
    .pivot("city_group_2", ["other", "group_b", "group_a"])
    .agg(F.sum("warehouse_capacity"))
    .orderBy("move_id")
)
