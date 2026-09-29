from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 2)
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

enriched = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("sk.weight_kg") <= 18.5)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("move_move_type_count"),
    )
)

profiled = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(~F.col("operator").isin("op_chan", "op_dew"), 1)).alias("move_operator_count"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

derived = (
    with_stats
    .select(
        "*",
        F.round(F.col("move_operator_count") / F.nullif(F.col("move_move_type_count"), F.lit(0)), 3).alias("move_operator_count_per_move_move_type_count"),
        F.substring(F.trim(F.col("warehouse_wh_code")), 1, 2).alias("warehouse_wh_code_prefix"),
    )
)

enriched_2 = (
    derived.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.sku_code",
        "s.sku_weight_kg",
        "s.warehouse_wh_code",
        "s.warehouse_opened_on",
        "s.move_move_type_count",
        "s.move_count",
        "s.move_operator_count",
        "s.move_operator_count_per_move_move_type_count",
        "s.warehouse_wh_code_prefix",
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "sku_code",
        "sku_weight_kg",
        "warehouse_wh_code",
        "move_move_type_count",
        "move_count",
        "move_operator_count",
        "move_operator_count_per_move_move_type_count",
        "warehouse_wh_code_prefix",
        "warehouse_opened_on_2",
        "sku_unit_cost",
        F.when(F.col("move_operator_count") >= 6, "HIGH").when(F.col("move_operator_count") >= 4, "MEDIUM").otherwise("LOW").alias("move_operator_count_tier"),
    )
)
