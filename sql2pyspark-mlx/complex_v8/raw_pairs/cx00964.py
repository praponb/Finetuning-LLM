from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

qualified = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") >= 111.7), "left_semi")
    .filter(F.col("qty") >= 191)
)

labeled = (
    qualified
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

labeled_2 = (
    labeled
    .select(
        "id",
        "sku_id",
        "qty",
        "operator_move_type_label",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.length(F.trim(F.col("operator_move_type_label"))).alias("operator_move_type_label_len"),
        F.lower(F.regexp_replace(F.trim(F.col("operator_move_type_label")), " ", "_")).alias("operator_move_type_label_slug"),
    )
)

stats = (
    labeled_2
    .groupBy(F.year("moved_at_week").alias("moved_at_week_year"))
    .agg(
        F.round(F.sum("operator_move_type_label_len") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("operator_move_type_label_len_to_qty_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("moved_at_week").alias("last_moved_at_week"),
    )
)

result = (
    stats
    .select(
        "moved_at_week_year",
        "operator_move_type_label_len_to_qty_ratio",
        "distinct_ids",
        "last_moved_at_week",
    )
    .orderBy(
        F.col("operator_move_type_label_len_to_qty_ratio").desc(),
        "moved_at_week_year",
    )
    .limit(10)
)
