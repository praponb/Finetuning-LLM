from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_moved_at_year_month_by_unit = Window.partitionBy(F.month("moved_at_year")).orderBy(F.col("unit_price").desc(), "id")
w_all_by_moved_at_year = Window.orderBy("moved_at_year", "id")
w_all_by_moved_at_year_running = Window.orderBy("moved_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.lead("qty").over(w_moved_at_year_month_by_unit).alias("next_qty"),
        F.dense_rank().over(w_all_by_moved_at_year).alias("moved_at_year_dense_rank"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.rank().over(w_moved_at_year_month_by_unit).alias("unit_price_rank"),
        F.sum("qty").over(w_all_by_moved_at_year_running).alias("running_qty"),
    )
)

with_activity = (
    with_trends
    .crossJoin(with_trends.agg(F.max("next_qty").alias("overall_max_next_qty")))
    .filter(F.col("next_qty") < F.col("overall_max_next_qty"))
    .drop("overall_max_next_qty")
)

result = (
    with_activity
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "moved_at_year",
        "distinct_sku_id_in_move_type",
        "next_qty",
        "rows_in_move_type",
        "running_qty",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy("sku_id")
)
