from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_id").orderBy(F.col("qty").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 293.2).alias("unit_price_distance"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

latest_per_key = (
    prepared_moves
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

line_level_moves = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

sku_id_stats = (
    line_level_moves
    .groupBy("sku_id")
    .agg(F.avg("move_unit_price").alias("avg_move_unit_price"))
)

vs_group = (
    line_level_moves.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.move_unit_price") > F.col("g.avg_move_unit_price"))
    .select(
        "s.*",
        "g.avg_move_unit_price",
        F.round(F.col("s.move_unit_price") - F.col("g.avg_move_unit_price"), 2).alias("move_unit_price_minus_avg"),
    )
)

result = (
    vs_group
    .rollup("move_id", "move_type", "warehouse_id")
    .agg(
        F.round(F.sum("move_unit_price_minus_avg"), 2).alias("total_move_unit_price_minus_avg"),
        F.round(F.stddev("move_unit_price"), 2).alias("stddev_move_unit_price"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
    )
    .select(
        "move_id",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "warehouse_id",
        "total_move_unit_price_minus_avg",
        "stddev_move_unit_price",
        "distinct_warehouse_ids",
    )
    .orderBy(
        F.col("move_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
