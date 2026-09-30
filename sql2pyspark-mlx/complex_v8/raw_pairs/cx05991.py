from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id_is_active_by_move_qt = Window.partitionBy("move_id", "is_active").orderBy("move_qty", "id", "move_id")
w_move_id_is_active_by_move_qt_2 = Window.partitionBy("move_id", "is_active").orderBy("move_qty")

recent_skus = (
    df_skus
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

annotated = (
    recent_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "is_active",
        F.when(F.col("unit_cost") >= 320.4, "HIGH").when(F.col("unit_cost") >= 134.3, "MEDIUM").otherwise("LOW").alias("unit_cost_tier"),
    )
)

with_children_moves = (
    annotated.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.unit_price").between(124.1, 361.9)), "inner")
    .select(
        "s.id",
        "s.sku_code",
        "s.category",
        "s.unit_cost",
        "s.is_active",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

ranked = (
    with_children_moves
    .select(
        "*",
        F.lag("move_qty", 2, 0).over(w_move_id_is_active_by_move_qt).alias("prev2_move_qty"),
        F.round(F.percent_rank().over(w_move_id_is_active_by_move_qt_2), 3).alias("move_qty_percent_rank"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.last_moved_at",
        "st.max_move_qty",
    )
)

with_parent_move = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("st.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.category",
        "s.unit_cost",
        "s.is_active",
        "s.move_id",
        "s.move_qty",
        "s.move_qty_percent_rank",
        "s.move_count",
        "s.last_moved_at",
        "s.max_move_qty",
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        "st.move_type",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

summarized_rows = (
    with_parent_move.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.first_moved_at")
)

result = (
    summarized_rows
    .groupBy("is_active", "move_count")
    .pivot("category", ["Toys", "Food"])
    .agg(F.max("move_unit_price_2").alias("max_v"), F.sum("move_unit_price_2").alias("sum_v"))
)
