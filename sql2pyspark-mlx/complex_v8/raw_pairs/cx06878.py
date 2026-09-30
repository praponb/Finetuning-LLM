from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("avg_move_qty", "id")

scoped_moves = (
    df_stock_moves
    .filter(F.col("sku_id") <= 15)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

latest_per_key = (
    scoped_moves
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

summarized_rows = (
    latest_per_key.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at")
)

labeled = (
    summarized_rows
    .select(
        "*",
        F.concat_ws(": ", F.col("move_type"), F.col("operator_ref")).alias("move_type_operator_ref_label"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
    )
)

profiled = (
    labeled.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.avg_move_qty",
    )
)

latest_per_key_2 = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

warehouse_id_stats = (
    latest_per_key_2
    .groupBy("warehouse_id")
    .agg(F.max("unit_price_filled").alias("max_unit_price_filled"))
)

vs_group = (
    latest_per_key_2.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price_filled") == F.col("g.max_unit_price_filled"))
    .select(
        "s.*",
        "g.max_unit_price_filled",
        F.round(F.col("s.unit_price_filled") - F.col("g.max_unit_price_filled"), 2).alias("unit_price_filled_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price_filled",
        "last_moved_at",
        "move_type_operator_ref_label",
        "first_moved_at",
        "avg_move_qty",
        "unit_price_filled_minus_max",
    )
    .orderBy("qty")
)
