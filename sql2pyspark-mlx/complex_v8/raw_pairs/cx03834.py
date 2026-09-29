from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_trends = (
    scoped_moves
    .select(
        "*",
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("qty_vs_warehouse_id_sku_id_avg"),
        F.lead("qty").over(w_move_type_by_qty).alias("next_qty"),
        F.nth_value("qty", 2).over(w_move_type_by_qty_full).alias("second_qty"),
    )
)

with_children_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.qty_vs_warehouse_id_sku_id_avg",
        "s.next_qty",
        "s.second_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

without_matches = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.id") == F.col("s.move_id")) & (F.col("st.moved_at") < F.lit("2025-03-02 13:00:00").cast("timestamp")) & (F.col("st.qty").between(99, 470)), "left_semi")
)

qualified = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("move_id") <= 88)
)

rollup_base = (
    qualified
    .groupBy("move_id", "move_move_type")
    .agg(
        F.countDistinct("move_id").alias("distinct_move_ids"),
        F.countDistinct("move_move_type").alias("distinct_move_move_types"),
    )
)

with_activity = (
    rollup_base.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.id") == F.col("s.move_id")) & (F.col("st.qty") < 300), "left_semi")
)

result = (
    with_activity
    .select(
        "move_id",
        "move_move_type",
        "distinct_move_ids",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
    )
)
