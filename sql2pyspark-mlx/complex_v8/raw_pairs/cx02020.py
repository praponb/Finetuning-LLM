from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(9, 15)) & (F.col("unit_price") <= 363.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

enriched = (
    valid_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "*",
        F.abs(F.col("qty") - 199).alias("qty_distance"),
        F.concat_ws(": ", F.col("move_type_slug"), F.col("move_type")).alias("move_type_slug_move_type_label"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type").isin("TRANSFER", "INBOUND", "ADJUST")) & (F.col("st.operator") != "op_ann"), "left_semi")
)

scored = (
    with_activity
    .withColumn("qty_per_qty_distance", F.round(F.col("qty") / F.nullif(F.col("qty_distance"), F.lit(0)), 3))
)

tagged = (
    scored
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "move_type_slug",
        "qty_distance",
        "move_type_slug_move_type_label",
        "qty_per_qty_distance",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.when(F.col("qty") >= 275, "A").when(F.col("qty") >= 191, "B").otherwise("C").alias("qty_tier"),
        F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_ann", 1)).alias("move_operator_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.first_moved_at",
    )
)

with_children_moves = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.move_type_slug",
        "s.move_type_slug_move_type_label",
        "s.move_type_or_unknown",
        "s.move_operator_count",
        "s.first_moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
    )
)

result = (
    with_children_moves
    .filter(F.col("sku_id") <= 13)
    .select(
        "id",
        "qty",
        "move_type_slug",
        "move_type_slug_move_type_label",
        "move_type_or_unknown",
        "first_moved_at",
        "move_id",
        "move_operator",
        "move_move_type",
        "move_moved_at",
        "move_qty",
        F.month(F.col("first_moved_at")).alias("first_moved_at_month"),
        F.least(F.col("move_qty"), F.col("qty")).alias("least_move_qty_qty"),
    )
    .orderBy(
        F.col("first_moved_at_month").desc(),
        F.col("move_id").desc_nulls_first(),
    )
)
