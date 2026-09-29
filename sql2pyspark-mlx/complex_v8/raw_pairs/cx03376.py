from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_qty = Window.orderBy(F.col("avg_qty").desc(), "moved_at_day", "moved_at_plus_15m_year")
w_all_by_avg_qty_running = Window.orderBy(F.col("avg_qty").desc(), "moved_at_day", "moved_at_plus_15m_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

scored = (
    scoped_moves
    .select(
        "*",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
)

summary = (
    scored
    .groupBy(
        F.to_date("moved_at").alias("moved_at_day"),
        F.year("moved_at_plus_15m").alias("moved_at_plus_15m_year"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("operator").isin("op_eve", "op_ann", "op_dew"), 1)).alias("n_operator_in_op_eve_op_ann_op"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.count(F.when(F.col("operator").isin("op_ann", "op_dew"), 1)).alias("n_operator_in_op_ann_op_dew"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.row_number().over(w_all_by_avg_qty).alias("rn"),
        F.round(F.cume_dist().over(w_all_by_avg_qty), 3).alias("avg_qty_cume_dist"),
        F.first("moved_at_plus_15m_year", ignorenulls=True).over(w_all_by_avg_qty_running).alias("first_known_moved_at_plus_15m_year"),
        F.round(F.percent_rank().over(w_all_by_avg_qty), 3).alias("avg_qty_percent_rank"),
        F.round(F.col("n_operator_in_op_ann_op_dew") - F.avg("n_operator_in_op_ann_op_dew").over(w_row_count), 2).alias("n_operator_in_op_ann_op_dew_vs_row_count_avg"),
    )
)

labeled = (
    with_windows
    .withColumn("days_since_moved_at_day", F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at_day")))
)

top_rows = labeled.filter((F.col("rn") == 1) & (F.col("n_operator_in_op_eve_op_ann_op") >= 2))

filtered = (
    top_rows
    .crossJoin(top_rows.agg(F.min("avg_qty").alias("overall_min_avg_qty")))
    .filter(F.col("avg_qty") > F.col("overall_min_avg_qty"))
    .drop("overall_min_avg_qty")
)

result = (
    filtered
    .select(
        "moved_at_day",
        "moved_at_plus_15m_year",
        "row_count",
        "n_operator_in_op_eve_op_ann_op",
        "avg_qty",
        "avg_qty_cume_dist",
        "first_known_moved_at_plus_15m_year",
        "avg_qty_percent_rank",
        "n_operator_in_op_ann_op_dew_vs_row_count_avg",
        "days_since_moved_at_day",
    )
)
