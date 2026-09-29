from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

labeled = (
    filtered_moves
    .select(
        "id",
        "sku_id",
        "operator",
        "days_since_moved_at",
        F.abs(F.col("unit_price") - 206.8).alias("unit_price_distance"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

with_children = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.unit_price_distance",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

scored = (
    with_children
    .withColumn("unit_price_distance_adj", F.round(F.col("unit_price_distance") * 1.1, 2))
)

per_group = (
    scored
    .groupBy("move_id", "move_type", "move_operator")
    .agg(
        F.sum(F.when(F.col("move_type") == "INBOUND", F.col("unit_price_distance_adj"))).alias("unit_price_distance_adj_move_type_inbound"),
        F.round(F.avg("unit_price_distance"), 2).alias("avg_unit_price_distance"),
        F.sum("unit_price_distance").alias("total_unit_price_distance"),
        F.sum("unit_price_distance_adj").alias("total_unit_price_distance_adj"),
        F.count(F.when(F.col("move_operator") != "op_dew", 1)).alias("n_move_operator_op_dew"),
        F.count("*").alias("cnt"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.min("unit_price_distance_adj_move_type_inbound").alias("overall_min_unit_price_distance_adj_move_type_in")))
    .filter(F.col("unit_price_distance_adj_move_type_inbound") > F.col("overall_min_unit_price_distance_adj_move_type_in"))
    .drop("overall_min_unit_price_distance_adj_move_type_in")
)

combined_details = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.id") == F.col("s.move_id")) & (F.col("st.move_type") == "ADJUST"), "left")
    .select("s.*", F.col("st.unit_price").alias("move_unit_price"))
)

result = (
    combined_details
    .select(
        "move_id",
        "move_operator",
        "unit_price_distance_adj_move_type_inbound",
        "total_unit_price_distance",
        "total_unit_price_distance_adj",
        "n_move_operator_op_dew",
        "cnt",
        F.round(F.col("move_unit_price") * 1.07, 2).alias("move_unit_price_adj"),
        F.substring(F.trim(F.col("move_operator")), 1, 2).alias("move_operator_prefix"),
    )
    .orderBy(F.col("move_unit_price_adj").asc_nulls_last())
)
