from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("qty") < 348) & (F.col("sku_id") <= 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

with_details = (
    profiled.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_count",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

enriched = (
    with_details
    .filter(F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp"))
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "move_count",
        "sku_labels",
        "warehouse_wh_code",
        "warehouse_city",
        F.coalesce(F.col("move_count"), F.lit(0)).alias("move_count_filled"),
        F.when(F.col("unit_price") >= 307.8, "HIGH").when(F.col("unit_price") >= 180.0, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_wh_code")), " ", "_")).alias("warehouse_wh_code_slug"),
    )
)

result = (
    enriched
    .groupingSets(
        [["move_count", "sku_id", "move_type"], ["move_count"], ["sku_id"]],
        "move_count", "sku_id", "move_type",
    )
    .agg(
        F.grouping("move_count").alias("is_total_move_count"),
        F.sum("move_count_filled").alias("total_move_count_filled"),
    )
    .orderBy(
        F.col("move_count").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
