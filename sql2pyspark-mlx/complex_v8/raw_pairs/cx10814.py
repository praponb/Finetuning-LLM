from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("qty") < 271)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

without_matches = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

labeled = (
    without_matches
    .select(
        "id",
        "move_type",
        "qty",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.abs(F.col("qty") - 154).alias("qty_distance"),
    )
)

annotated = (
    labeled
    .filter(F.col("qty_distance") <= 425)
    .select(
        "id",
        "move_type",
        "qty",
        "unit_price",
        "moved_at_label",
        "qty_distance",
        F.lower(F.trim(F.col("moved_at_label"))).alias("moved_at_label_clean"),
        F.greatest(F.col("qty_distance"), F.col("qty")).alias("greatest_qty_distance_qty"),
    )
)

stats = (
    annotated
    .groupBy("move_type")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("n_move_type_inbound"),
        F.round(F.sum("qty") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_to_unit_price_ratio"),
        F.sum(F.when(F.col("move_type") != "OUTBOUND", F.col("qty")).otherwise(0)).alias("qty_move_type_outbound"),
    )
)

result = (
    stats
    .rollup("n_move_type_inbound", "move_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list_2"),
        F.round(100 * F.count(F.when(F.col("move_type") == "TRANSFER", 1)) / F.count("*"), 1).alias("pct_move_type_transfer"),
    )
    .orderBy(
        F.col("n_move_type_inbound").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
