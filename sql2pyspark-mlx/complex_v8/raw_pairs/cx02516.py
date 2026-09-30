from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_move_unit_price = Window.orderBy("move_unit_price", "id", "move_id")
w_move_id = Window.partitionBy("move_id")
w_all_by_move_unit_price_runni = Window.orderBy("move_unit_price", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined_ranges = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.qty").alias("prior_qty_3d"),
    )
)

with_children = (
    joined_ranges.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 289.7), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_3d",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

with_windows = (
    with_children
    .select(
        "*",
        F.row_number().over(w_all_by_move_unit_price).alias("seq_no"),
        F.count(F.lit(1)).over(w_move_id).alias("rows_in_move_id"),
        F.count(F.lit(1)).over(w_all_by_move_unit_price_runni).alias("running_count"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(~F.col("move_type").isin("TRANSFER", "INBOUND"), 1)).alias("move_move_type_count"),
        F.min("moved_at").alias("first_moved_at"),
        F.min("moved_at").alias("first_moved_at_2"),
    )
)

with_counts = (
    with_windows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.first_moved_at",
        "st.first_moved_at_2",
    )
)

result = (
    with_counts
    .groupBy("first_moved_at", "sku_id")
    .pivot("move_operator", ["op_dew", "op_ann", "op_eve"])
    .agg(F.count("rows_in_move_id").alias("count_v"), F.sum("rows_in_move_id").alias("sum_v"))
)
