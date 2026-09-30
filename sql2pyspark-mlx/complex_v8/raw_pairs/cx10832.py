from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_sku_id_prior_rows_3d = Window.partitionBy("sku_id", "prior_rows_3d")
w_all_by_moved_at_moving_6 = Window.orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.month("moved_at"))
w_moved_at_month_last_known_pr = Window.partitionBy(F.date_trunc("month", "moved_at"), "last_known_prior_rows_3d").orderBy("moved_at", "id").rowsBetween(-1, Window.currentRow)

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
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

windowed = (
    joined_ranges
    .select(
        "*",
        F.last("prior_rows_3d", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_prior_rows_3d"),
        F.max("prior_rows_3d").over(w_all_by_moved_at_running).alias("max_prior_rows_3d_so_far"),
        F.lead("prior_unit_price_3d", 1, 0).over(w_all_by_moved_at).alias("next_prior_unit_price_3d"),
        F.min("prior_unit_price_3d").over(w_sku_id_prior_rows_3d).alias("sku_id_prior_rows_3d_min_prior_unit_price_3d"),
        F.round(F.percent_rank().over(w_all_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.round(F.avg("prior_rows_3d").over(w_all_by_moved_at_moving_6), 2).alias("prior_rows_3d_avg_7"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id_moved_at_month).alias("rows_in_sku_id_moved_at_month"),
        F.sum("prior_rows_3d").over(w_moved_at_month_last_known_pr).alias("prior_rows_3d_sum_2"),
        F.sum("max_prior_rows_3d_so_far").over(w_sku_id_moved_at_month).alias("sku_id_moved_at_month_sum_max_prior_rows_3d_so_f"),
    )
)

result = (
    sequenced
    .rollup("sku_id", "prior_rows_3d", "last_known_prior_rows_3d")
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("prior_rows_3d").asc_nulls_last(),
        F.col("last_known_prior_rows_3d").asc_nulls_last(),
    )
)
