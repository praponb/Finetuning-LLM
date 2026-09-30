from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

with_parent_sku = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at_dayofweek",
        "s.operator_slug",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.count("*").alias("move_count_3"),
    )
)

with_stats = (
    with_parent_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        F.coalesce(F.col("st.move_count_3"), F.lit(0)).alias("move_count_3"),
    )
)

line_level = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.sku_unit_cost",
        "s.warehouse_capacity",
        "s.move_count",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.operator").alias("move_operator"),
        "st.move_type",
    )
)

joined_sku = (
    line_level.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.sku_unit_cost",
        "s.warehouse_capacity",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_operator",
        "s.move_type",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("qty").alias("min_move_qty"))
)

with_stats_2 = (
    joined_sku.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.min_move_qty")
)

enriched = (
    with_stats_2
    .select(
        "*",
        F.least(F.col("sku_unit_cost"), F.col("warehouse_capacity")).alias("least_sku_unit_cost_warehouse_capacity"),
        F.substring(F.trim(F.col("sku_category")), 1, 3).alias("sku_category_prefix"),
    )
)

result = (
    enriched
    .select(
        "id",
        "move_id",
        "least_sku_unit_cost_warehouse_capacity",
        "sku_unit_cost",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["least_sku_unit_cost_warehouse_capacity", "sku_unit_cost"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
