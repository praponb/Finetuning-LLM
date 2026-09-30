from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_dew")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
    )
)

screened = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") > 58.6), "left_anti")
)

filtered = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%nx%")) & (F.col("w.capacity") >= 4912), "left_semi")
)

enriched_warehouse = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    enriched_warehouse
    .rollup("sku_id", "operator", "warehouse_city")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("moved_at_plus_1d").alias("first_moved_at_plus_1d"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("warehouse_city").asc_nulls_last(),
    )
)
