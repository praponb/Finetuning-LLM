from pyspark.sql import functions as F

base = (
    df_skus
    .filter((F.col("unit_cost") > 134.3) & (~F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

enriched = (
    base
    .filter(F.col("weight_kg") <= 20.9)
    .select(
        "*",
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
        F.abs(F.col("unit_cost") - 239.3).alias("unit_cost_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("INBOUND", "OUTBOUND", "TRANSFER"), 1)).alias("move_move_type_count"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        "category_or_unknown",
        "unit_cost_distance",
        F.round(F.col("unit_cost") / F.nullif(F.col("unit_cost_distance"), F.lit(0)), 3).alias("unit_cost_per_unit_cost_distance"),
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
    .orderBy(
        F.col("category_or_unknown").asc_nulls_last(),
        F.col("unit_cost").desc_nulls_first(),
    )
)
