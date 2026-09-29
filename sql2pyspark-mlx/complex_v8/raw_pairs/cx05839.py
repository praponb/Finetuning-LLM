from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("operator").isin("op_dew", "op_ann")) & (F.col("moved_at") >= F.lit("2025-02-06 07:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

combined_details = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

exploded = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "warehouse_city",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "sku_code",
        F.explode("sku_labels").alias("sku_label"),
    )
)

rollup_base = (
    exploded
    .filter(F.col("sku_id").isNotNull())
    .groupBy("warehouse_city")
    .agg(
        F.count(F.when(F.col("warehouse_wh_code").isNotNull(), 1)).alias("n_warehouse_wh_code_is_not_nul"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

labeled = (
    rollup_base
    .select(
        "warehouse_city",
        "n_warehouse_wh_code_is_not_nul",
        F.round(F.col("max_unit_price") * 0.25, 2).alias("max_unit_price_adj"),
    )
)

result = (
    labeled
    .select(
        "warehouse_city",
        "n_warehouse_wh_code_is_not_nul",
        "max_unit_price_adj",
        F.when(F.col("max_unit_price_adj") >= 487.9, "HIGH").when(F.col("max_unit_price_adj") >= 263.1, "MEDIUM").otherwise("LOW").alias("max_unit_price_adj_tier"),
        F.coalesce(F.col("max_unit_price_adj"), F.lit(0.0)).alias("max_unit_price_adj_filled"),
    )
    .orderBy(F.col("max_unit_price_adj").asc(), "warehouse_city")
    .limit(10)
)
