from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("unit_price") >= 161.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_bee", 1)).alias("move_operator_count"),
        F.count(F.when(F.col("operator").isin("op_ann", "op_bee", "op_dew"), 1)).alias("move_operator_count_2"),
        F.min("qty").alias("min_move_qty"),
    )
)

profiled = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_operator_count_2"), F.lit(0)).alias("move_operator_count_2"),
        "st.min_move_qty",
    )
)

enriched = (
    profiled.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_operator_count",
        "s.move_operator_count_2",
        "s.min_move_qty",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

annotated = (
    enriched
    .select(
        "*",
        F.coalesce(F.col("move_operator_count"), F.lit(0)).alias("move_operator_count_filled"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.concat(F.col("operator"), F.lit("#"), F.col("min_move_qty").cast("string")).alias("operator_ref"),
    )
)

with_parent = (
    annotated.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.lower(F.col("sk.sku_code")).like("%p-%"))
    .select("s.*", "sk.sku_code", F.col("w.city").alias("warehouse_city_2"))
)

derived = (
    with_parent
    .filter(F.col("sku_category") != "Apparel")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "move_operator_count_2",
        "min_move_qty",
        "sku_is_active",
        "sku_weight_kg",
        "sku_category",
        "moved_at_label",
        "operator_ref",
        "sku_code",
        F.when(F.col("warehouse_city").isin("Kuala Lumpur", "Bangkok"), "group_a").when(F.col("warehouse_city") == "Singapore", "group_b").otherwise("other").alias("warehouse_city_group"),
        F.last_day(F.col("warehouse_opened_on")).alias("warehouse_opened_on_month_end"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    derived.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_counts
    .select(
        "id",
        "move_count",
        "min_move_qty",
        "move_operator_count_2",
    )
    .unpivot(
        ids=["id"],
        values=["move_count", "min_move_qty", "move_operator_count_2"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
