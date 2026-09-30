from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy(F.col("qty").desc(), "id")
w_moved_at_month_warehouse_id_2 = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_warehouse_id_3 = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-3, -1)

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

windowed = (
    scoped_moves
    .select(
        "*",
        F.round(F.col("unit_price_filled") / F.nullif(F.sum("unit_price_filled").over(w_operator_sku_id), F.lit(0)), 4).alias("unit_price_filled_share_of_operator_sku_id"),
        F.datediff(F.col("moved_at_date"), F.lag("moved_at_date").over(w_moved_at_month_warehouse_id)).alias("days_since_prev_moved_at_date"),
        F.last("unit_price_filled").over(w_moved_at_month_warehouse_id_2).alias("last_unit_price_filled"),
        F.round(F.avg("unit_price_filled").over(w_moved_at_month_warehouse_id_3), 2).alias("unit_price_filled_avg_3_prior"),
        (F.col("unit_price_filled") - F.lag("unit_price_filled").over(w_moved_at_month_warehouse_id)).alias("unit_price_filled_change"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    windowed.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_stats
    .groupingSets(
        [["move_count", "operator"], ["move_count"], []],
        "move_count", "operator",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("operator").isin("op_ann", "op_dew"), 1).otherwise(0)).alias("n_operator_in_op_ann_op_dew"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.stddev("last_unit_price_filled"), 2).alias("stddev_last_unit_price_filled"),
    )
)
