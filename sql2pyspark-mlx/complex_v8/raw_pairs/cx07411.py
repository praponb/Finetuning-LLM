from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 313.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

with_parent = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

enriched = (
    with_parent
    .select(
        "id",
        "sku_id",
        "move_type",
        "sku_is_active",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.min("unit_price").alias("min_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.min_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["move_type", "sku_is_active", "sku_id"], ["move_type"]],
        "move_type", "sku_is_active", "sku_id",
    )
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.sum(F.when(~F.col("move_type").isin("INBOUND", "ADJUST"), F.col("move_count")).otherwise(0)).alias("move_count_move_type_not_in_inbound_adj"),
        F.sum("move_count").alias("total_move_count"),
        F.min("moved_at_year").alias("min_moved_at_year"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("sku_is_active").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
