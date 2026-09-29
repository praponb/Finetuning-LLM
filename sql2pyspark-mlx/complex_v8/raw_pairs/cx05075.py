from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_median_move_qty = Window.partitionBy("operator").orderBy("median_move_qty")
w_operator_by_median_move_qty_2 = Window.partitionBy("operator").orderBy("median_move_qty", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_median_move_qty_3 = Window.partitionBy("operator").orderBy("median_move_qty", "id", "move_id")
w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))

scoped = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

warehouse_id_stats = (
    detail_rows
    .groupBy("warehouse_id")
    .agg(F.percentile_approx("move_qty", 0.5).alias("median_move_qty"))
)

vs_group = (
    detail_rows.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.move_qty") >= F.col("g.median_move_qty"))
    .select(
        "s.*",
        "g.median_move_qty",
        F.round(F.col("s.move_qty") - F.col("g.median_move_qty"), 2).alias("move_qty_minus_median"),
    )
)

metrics = (
    vs_group
    .select(
        "*",
        F.dense_rank().over(w_operator_by_median_move_qty).alias("median_move_qty_dense_rank"),
        F.min("median_move_qty").over(w_operator_by_median_move_qty_2).alias("min_median_move_qty_so_far"),
        F.lag("move_qty", 2, 0).over(w_operator_by_median_move_qty_3).alias("prev2_move_qty"),
        F.count(F.lit(1)).over(w_moved_at_month).alias("rows_in_moved_at_month"),
    )
)

result = (
    metrics
    .groupBy("move_id")
    .pivot("operator", ["op_dew", "op_bee", "op_eve"])
    .agg(F.max("move_qty").alias("max_v"), F.count("move_qty").alias("n"))
)
