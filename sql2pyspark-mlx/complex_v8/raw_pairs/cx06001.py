from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("operator").isin("op_chan", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.lower(F.col("sk.sku_code")).like("%20%")), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.upper(F.trim(F.col("sk.sku_code"))).like("E%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.least_qty_unit_price",
        "s.moved_at_date",
        "sk.sku_code",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

filtered_2 = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-12").cast("date")) & (F.col("st.warehouse_id").isNotNull()), "left_semi")
)

joined = (
    filtered_2.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") >= 8.5), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.least_qty_unit_price",
        "s.moved_at_date",
        "s.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.sku_code").alias("sku_sku_code"),
    )
)

screened = (
    joined.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") > 85) & (F.col("st.sku_id") <= 15), "left_anti")
    .filter(F.col("sku_code").isNotNull())
)

result = (
    screened
    .rollup("warehouse_id", "sku_is_active", "sku_id")
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.count("*").alias("n_rows"),
    )
)
