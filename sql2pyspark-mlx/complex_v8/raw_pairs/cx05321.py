from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

detail_rows = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

combined_details = (
    detail_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.move_id",
        "s.move_sku_id",
        "s.move_operator",
        "s.move_unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.category").alias("sku_category"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") > 99) & (F.col("st.warehouse_id") >= 5), "left_semi")
    .filter(F.col("move_sku_id") <= 11)
)

enriched_move = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_id",
        "s.move_sku_id",
        "s.move_operator",
        "s.move_unit_price",
        "s.warehouse_capacity",
        "s.warehouse_wh_code",
        "s.sku_category",
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator_2"),
        F.col("st.sku_id").alias("move_sku_id_2"),
    )
)

rollup_base = (
    enriched_move
    .groupBy("move_sku_id_2", "move_operator_2", "move_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.min("moved_at").alias("first_moved_at"),
        F.countDistinct("move_id").alias("distinct_move_ids"),
    )
)

labeled = (
    rollup_base
    .filter(F.hour(F.col("first_moved_at")).between(10, 12))
    .select(
        "*",
        (F.col("first_moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("first_moved_at_plus_30m"),
        F.round(F.col("distinct_sku_ids") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("distinct_sku_ids_per_distinct_ids"),
    )
)

result = (
    labeled
    .select(
        "move_sku_id_2",
        "move_operator_2",
        "distinct_ids",
        "distinct_sku_ids",
        "first_moved_at",
        "distinct_move_ids",
        "first_moved_at_plus_30m",
        F.to_date(F.col("first_moved_at")).alias("first_moved_at_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_moved_at")).alias("days_since_first_moved_at"),
    )
    .orderBy(F.col("first_moved_at").desc())
)
