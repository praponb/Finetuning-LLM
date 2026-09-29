from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

exploded_detail = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_date",
        "s.operator_or_unknown",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

enriched = (
    exploded_detail.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.moved_at_date",
        "s.operator_or_unknown",
        "s.move_id",
        "s.move_qty",
        "s.move_unit_price",
        "s.move_warehouse_id",
        F.col("st.operator").alias("move_operator"),
    )
)

joined_sku = (
    enriched.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

operator_stats = (
    joined_sku
    .groupBy("operator")
    .agg(F.avg("sku_weight_kg").alias("avg_sku_weight_kg"))
)

outliers = (
    joined_sku.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.sku_weight_kg") > F.col("g.avg_sku_weight_kg"))
    .select(
        "s.*",
        "g.avg_sku_weight_kg",
        F.round(F.col("s.sku_weight_kg") - F.col("g.avg_sku_weight_kg"), 2).alias("sku_weight_kg_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at_date",
        "move_id",
        "move_qty",
        "move_unit_price",
        "move_operator",
        "sku_weight_kg",
        "avg_sku_weight_kg",
        "sku_weight_kg_minus_avg",
        F.date_add(F.col("moved_at_date"), 90).alias("moved_at_date_plus_90d"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), "avg_sku_weight_kg")
)
