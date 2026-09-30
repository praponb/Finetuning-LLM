from pyspark.sql import functions as F

scoped_skus = (
    df_skus
    .filter(F.array_contains(F.col("labels"), "cold"))
    .select(
        "id",
        "sku_code",
        "labels",
        "is_active",
        F.substring(F.trim(F.col("sku_code")), 1, 2).alias("sku_code_prefix"),
        F.greatest(F.col("unit_cost"), F.col("weight_kg")).alias("greatest_unit_cost_weight_kg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

summarized_rows = (
    scoped_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at")
)

without_matches = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (~F.col("st.move_type").isin("OUTBOUND", "INBOUND")), "left_semi")
)

scored = (
    without_matches
    .filter(~F.col("is_active"))
    .select(
        "*",
        F.length(F.trim(F.col("sku_code_prefix"))).alias("sku_code_prefix_len"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.count(F.when(F.col("operator") != "op_eve", 1)).alias("move_operator_count"),
    )
)

with_counts = (
    scored.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

screened = (
    with_counts
    .crossJoin(with_counts.agg(F.max("total_move_qty").alias("overall_max_total_move_qty")))
    .filter(F.col("total_move_qty") < F.col("overall_max_total_move_qty"))
    .drop("overall_max_total_move_qty")
)

result = (
    screened
    .select(
        "id",
        "sku_code",
        "labels",
        "is_active",
        "sku_code_prefix",
        "greatest_unit_cost_weight_kg",
        "last_moved_at",
        "sku_code_prefix_len",
        "total_move_qty",
        "move_operator_count",
        F.least(F.col("total_move_qty"), F.col("move_operator_count")).alias("least_total_move_qty_move_operator_count"),
        F.lower(F.regexp_replace(F.trim(F.col("sku_code_prefix")), " ", "_")).alias("sku_code_prefix_slug"),
    )
    .orderBy(F.col("sku_code_prefix_len").desc())
)
