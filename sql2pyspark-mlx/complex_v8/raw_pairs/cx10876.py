from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("unit_price") >= 130.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_unit_price",
        "st.last_moved_at",
        "st.first_moved_at",
    )
)

result = (
    summarized_rows
    .cube("move_type", "warehouse_id")
    .agg(
        F.sum("days_since_moved_at").alias("total_days_since_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
        F.countDistinct("move_type").alias("distinct_move_types"),
    )
)
