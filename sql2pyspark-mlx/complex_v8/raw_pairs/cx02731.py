from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")) | (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

profiled = (
    valid_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

scored = (
    profiled
    .select(
        "*",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("operator_ref")), " ", "_")).alias("operator_ref_slug"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.col("move_type").isin("OUTBOUND", "TRANSFER", "INBOUND"))
)

result = (
    qualified
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator_ref",
        "unit_price_bucket",
        "move_count",
        "move_count_2",
        "moved_at_month",
        "operator_ref_slug",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.concat_ws(": ", F.col("operator_ref"), F.col("operator_ref_slug")).alias("operator_ref_operator_ref_slug_label"),
    )
    .orderBy("qty", F.col("move_count_2").desc())
)
