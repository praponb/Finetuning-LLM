from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_qty_running = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_operator_by_qty = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id")

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

metrics = (
    base
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.min("qty").over(w_operator_by_qty_running).alias("min_qty_so_far"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.row_number().over(w_operator_by_qty).alias("seq_no"),
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
        F.round(F.avg("qty").over(w_move_type), 2).alias("move_type_avg_qty"),
    )
)

grouped = (
    metrics
    .groupBy("sku_id", "move_type", "seq_no")
    .agg(
        F.count(F.when(F.col("move_type").isin("OUTBOUND", "TRANSFER"), 1)).alias("n_move_type_in_outbound_transf"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("operator").isNotNull(), 1)) / F.count("*"), 1).alias("pct_operator_is_not_null"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    grouped
    .select(
        "sku_id",
        "move_type",
        "seq_no",
        "pct_operator_is_not_null",
        "avg_unit_price",
    )
)
