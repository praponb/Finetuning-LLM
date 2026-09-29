from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price_bucket").desc(), "id")
w_move_operator_count_sku_id = Window.partitionBy("move_operator_count", "sku_id")
w_dedup = Window.partitionBy("warehouse_id").orderBy("avg_move_unit_price", "id", "move_id")

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_bee", 1)).alias("move_operator_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.min("qty").alias("min_move_qty"),
    )
)

summarized_rows = (
    valid_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.avg_move_unit_price",
        "st.min_move_qty",
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.row_number().over(w_sku_id_operator_by_unit_pric).alias("rn"),
        F.min("unit_price").over(w_move_operator_count_sku_id).alias("move_operator_count_sku_id_min_unit_price"),
        F.round(F.avg("unit_price").over(w_move_operator_count_sku_id), 2).alias("move_operator_count_sku_id_avg_unit_price"),
        F.max("avg_move_unit_price").over(w_move_operator_count_sku_id).alias("move_operator_count_sku_id_max_avg_move_unit_pri"),
        F.round(F.col("min_move_qty") / F.nullif(F.sum("min_move_qty").over(w_move_operator_count_sku_id), F.lit(0)), 4).alias("min_move_qty_share_of_move_operator_count_sku_id"),
    )
)

with_children_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        "s.move_operator_count",
        "s.avg_move_unit_price",
        "s.rn",
        "s.min_move_qty_share_of_move_operator_count_sku_id",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

unique_rows = (
    with_children_moves
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.sum("qty").alias("total_move_qty"))
)

with_counts = (
    unique_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.total_move_qty")
)

result = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "moved_at_label",
        "rn",
        "min_move_qty_share_of_move_operator_count_sku_id",
        "move_unit_price",
        "move_type",
        "move_moved_at",
    )
    .orderBy("warehouse_id", F.col("move_type").desc())
)
