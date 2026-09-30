from pyspark.sql import functions as F

recent_skus = (
    df_skus
    .filter(F.col("weight_kg") <= 14.6)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.array_contains(F.col("labels"), "cold").alias("has_cold"),
    )
)

with_activity = (
    recent_skus.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), 1)).alias("move_move_type_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

rollup_base = (
    summarized_rows
    .groupBy(
        F.to_date("last_moved_at").alias("last_moved_at_day"),
        F.year("first_moved_at").alias("first_moved_at_year"),
        "has_cold",
    )
    .agg(
        F.min("move_move_type_count").alias("min_move_move_type_count"),
        F.count(F.when(F.col("category").isin("Beauty", "Apparel", "Toys"), 1)).alias("n_category_in_beauty_apparel_t"),
    )
)

result = (
    rollup_base
    .select(
        "last_moved_at_day",
        "first_moved_at_year",
        "has_cold",
        "min_move_move_type_count",
        "n_category_in_beauty_apparel_t",
        (F.col("min_move_move_type_count") - F.col("first_moved_at_year")).alias("min_move_move_type_count_minus_first_moved_at_ye"),
        F.abs(F.col("first_moved_at_year") - 2025).alias("first_moved_at_year_distance"),
    )
)
