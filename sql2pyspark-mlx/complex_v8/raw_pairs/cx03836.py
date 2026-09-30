from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_move_qty_full = Window.orderBy(F.col("move_qty").asc_nulls_last(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_move_id = Window.partitionBy("move_type", "move_id")
w_all_by_move_qty = Window.orderBy(F.col("move_qty").asc_nulls_last(), "id", "move_id")
w_all_by_move_qty_ties = Window.orderBy(F.col("move_qty").asc_nulls_last())

filtered_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(10, 16))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

with_activity = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

with_children = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (~F.col("st.operator").isin("op_dew", "op_eve")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.operator_slug",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

with_windows = (
    with_children
    .select(
        "*",
        F.first("unit_price").over(w_all_by_move_qty_full).alias("first_unit_price"),
        F.count(F.lit(1)).over(w_move_type_move_id).alias("rows_in_move_type_move_id"),
        F.dense_rank().over(w_all_by_move_qty).alias("move_qty_dense_rank"),
        F.row_number().over(w_all_by_move_qty).alias("move_qty_seq"),
        F.dense_rank().over(w_all_by_move_qty_ties).alias("move_qty_dense_rank_2"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        F.abs(F.col("move_qty_seq") - 4).alias("move_qty_seq_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at_2"),
    )
)

result = (
    annotated
    .groupingSets(
        [["warehouse_id", "move_qty_seq", "move_type"], ["warehouse_id"], ["move_qty_seq"]],
        "warehouse_id", "move_qty_seq", "move_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.count("*").alias("cnt"),
    )
)
