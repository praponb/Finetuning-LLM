from pyspark.sql import functions as F

scoped_skus = (
    df_skus
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.lower(F.trim(F.col("category"))).alias("category_clean"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_counts = (
    scoped_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.avg_move_unit_price",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["move_count", "is_active"], ["move_count"], ["is_active"]],
        "move_count", "is_active",
    )
    .agg(
        F.sum("avg_move_unit_price").alias("total_avg_move_unit_price"),
        F.percentile_approx("move_count", 0.5).alias("median_move_count"),
    )
)
