from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at"))
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter((F.col("unit_price") < 372.6) | (F.hour(F.col("moved_at")).between(6, 9)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("OUTBOUND", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
)

with_windows = (
    recent_moves
    .withColumn("unit_price_vs_sku_id_moved_at_month_avg", F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_moved_at_month), 2))
    .withColumn("min_unit_price_so_far", F.min("unit_price").over(w_all_by_qty_running))
)

result = (
    with_windows
    .rollup("sku_id", "move_type_group")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("operator") == "op_eve", 1)) / F.count("*"), 1).alias("pct_operator_op_eve"),
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
    )
)
