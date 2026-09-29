from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id_sku_id_by_move_moved = Window.partitionBy("move_id", "sku_id").orderBy("move_moved_at")
w_move_moved_at_year_by_move_m = Window.partitionBy(F.year("move_moved_at")).orderBy("move_moved_at")

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

without_matches = (
    cleaned
    .crossJoin(cleaned.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

line_level = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_ann", "op_chan", "op_eve")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

metrics = (
    line_level
    .select(
        "id",
        "warehouse_id",
        "move_id",
        "move_operator",
        "move_qty",
        "move_moved_at",
        F.dense_rank().over(w_move_id_sku_id_by_move_moved).alias("move_moved_at_dense_rank"),
        F.dense_rank().over(w_move_moved_at_year_by_move_m).alias("move_moved_at_dense_rank_2"),
    )
)

result = (
    metrics
    .groupBy("move_moved_at", "move_moved_at_dense_rank_2")
    .pivot("move_operator", ["op_bee", "op_dew", "op_eve", "op_chan"])
    .agg(F.sum("move_moved_at_dense_rank"))
    .orderBy("move_moved_at", "move_moved_at_dense_rank_2")
)
