from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("qty") < 372)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

qualified = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.move_type").isin("INBOUND", "TRANSFER")) & (F.col("st.unit_price") <= 293.2), "left_semi")
)

screened = (
    qualified
    .crossJoin(qualified.agg(F.avg("qty").alias("overall_avg_qty")))
    .filter(F.col("qty") > F.col("overall_avg_qty"))
    .drop("overall_avg_qty")
)

with_details_sku = (
    screened.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.array_contains(F.col("sk.labels"), "cold"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.days_since_moved_at",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    with_details_sku
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "days_since_moved_at",
        "warehouse_city",
        "warehouse_wh_code",
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
        F.upper(F.trim(F.col("warehouse_city"))).alias("warehouse_city_clean"),
    )
    .orderBy("warehouse_wh_code")
)
