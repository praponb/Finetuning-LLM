from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.when(F.col("move_type").isin("ADJUST", "OUTBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") <= 275), "left_semi")
)

enriched = (
    without_matches.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.capacity") <= 11506)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.operator_slug",
        "s.move_type_group",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isNotNull()), "left_anti")
)

rollup_base = (
    filtered
    .filter(F.col("sku_category") != "Toys")
    .groupBy("operator")
    .agg(
        F.countDistinct("move_type_group").alias("distinct_move_type_groups"),
        F.count(F.when(F.col("sku_category").isin("Toys", "Electronics", "Apparel"), 1)).alias("n_sku_category_in_toys_electro"),
    )
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("distinct_move_type_groups").alias("overall_avg_distinct_move_type_groups")))
    .filter(F.col("distinct_move_type_groups") > F.col("overall_avg_distinct_move_type_groups"))
    .drop("overall_avg_distinct_move_type_groups")
)

result = (
    with_activity
    .select(
        "operator",
        "distinct_move_type_groups",
        "n_sku_category_in_toys_electro",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy(F.col("distinct_move_type_groups").desc())
)
