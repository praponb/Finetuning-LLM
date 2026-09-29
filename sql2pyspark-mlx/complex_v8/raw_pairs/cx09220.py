from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type_or_unknown").orderBy(F.col("move_count").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator") != "op_chan") & (F.col("unit_price") <= 277.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.abs(F.col("unit_price") - 292.3).alias("unit_price_distance"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

enriched = (
    scoped_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price_adj",
        F.coalesce(F.col("unit_price_distance"), F.lit(0.0)).alias("unit_price_distance_filled"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.abs(F.col("unit_price_adj") - 412.9).alias("unit_price_adj_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

latest_per_key = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_children = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.operator",
        "s.unit_price_adj",
        "s.unit_price_distance_filled",
        "s.unit_price_adj_distance",
        "s.min_move_unit_price",
        "s.move_count",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

result = (
    with_children
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price_distance_filled",
        "move_count",
        "move_id",
        "move_moved_at",
        "move_sku_id",
        F.coalesce(F.col("move_count"), F.lit(0)).alias("move_count_filled"),
        F.to_date(F.col("move_moved_at")).alias("move_moved_at_date"),
    )
    .orderBy(
        F.col("unit_price_distance_filled").asc_nulls_last(),
        F.col("move_count_filled").asc_nulls_last(),
    )
)
