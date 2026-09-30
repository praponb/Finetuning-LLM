from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_plus_15m_month = Window.partitionBy(F.month("moved_at_plus_15m"))
w_operator_by_qty_running = Window.partitionBy("operator").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy(F.col("qty").asc_nulls_last())
w_qty_dense_rank_warehouse_id = Window.partitionBy("qty_dense_rank", "warehouse_id")
w_all_by_qty_vs_moved_at_plus = Window.orderBy(F.col("qty_vs_moved_at_plus_15m_month_avg").desc(), "id")

scoped = (
    df_stock_moves
    .filter(F.col("qty") < 349)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        F.round(F.col("qty") - F.avg("qty").over(w_moved_at_plus_15m_month), 2).alias("qty_vs_moved_at_plus_15m_month_avg"),
        F.first("qty", ignorenulls=True).over(w_operator_by_qty_running).alias("first_known_qty"),
        F.dense_rank().over(w_operator_by_qty_ties).alias("qty_dense_rank"),
    )
)

screened = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        "warehouse_id",
        "qty_vs_moved_at_plus_15m_month_avg",
        "first_known_qty",
        "qty_dense_rank",
        F.size(F.collect_set("move_type").over(w_qty_dense_rank_warehouse_id)).alias("distinct_move_type_in_qty_dense_rank_warehouse_i"),
        F.ntile(5).over(w_all_by_qty_vs_moved_at_plus).alias("qty_vs_moved_at_plus_15m_month_avg_quintile"),
    )
)

result = (
    windowed
    .rollup("distinct_move_type_in_qty_dense_rank_warehouse_i", "warehouse_id", "qty_vs_moved_at_plus_15m_month_avg_quintile")
    .agg(
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.stddev("first_known_qty"), 2).alias("stddev_first_known_qty"),
    )
    .orderBy(
        F.col("distinct_move_type_in_qty_dense_rank_warehouse_i").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("qty_vs_moved_at_plus_15m_month_avg_quintile").asc_nulls_last(),
    )
)
