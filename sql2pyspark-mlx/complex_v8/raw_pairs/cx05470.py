from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d = Window.partitionBy("prior_rows_3d")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_prior_rows_3d_moved_at_month = Window.partitionBy("prior_rows_3d", F.date_trunc("month", "moved_at")).orderBy(F.col("prior_unit_price_3d").asc_nulls_last(), "id")
w_all_by_rows_in_prior_rows_3d = Window.orderBy(F.col("rows_in_prior_rows_3d").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_prev_prior_rows_3d = Window.orderBy(F.col("prev_prior_rows_3d").desc_nulls_first(), "id")
w_all_by_prev_prior_rows_3d_mo = Window.orderBy(F.col("prev_prior_rows_3d").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)

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
        "id",
        "moved_at",
        "prior_rows_3d",
        "prior_unit_price_3d",
        F.count(F.lit(1)).over(w_prior_rows_3d).alias("rows_in_prior_rows_3d"),
        F.lag("prior_rows_3d").over(w_sku_id_by_moved_at).alias("prev_prior_rows_3d"),
        F.min("prior_unit_price_3d").over(w_prior_rows_3d).alias("prior_rows_3d_min_prior_unit_price_3d"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_prior_rows_3d_moved_at_month))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "rows_in_prior_rows_3d",
        "prev_prior_rows_3d",
        F.round(F.avg("prev_prior_rows_3d").over(w_all_by_rows_in_prior_rows_3d), 2).alias("prev_prior_rows_3d_avg_3_prior"),
        (F.col("prior_rows_3d") - F.lag("prior_rows_3d").over(w_all_by_prev_prior_rows_3d)).alias("prior_rows_3d_change"),
        F.max("rows_in_prior_rows_3d").over(w_all_by_prev_prior_rows_3d_mo).alias("rows_in_prior_rows_3d_max_4"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "rows_in_prior_rows_3d",
        "prev_prior_rows_3d",
        "prev_prior_rows_3d_avg_3_prior",
        "prior_rows_3d_change",
        "rows_in_prior_rows_3d_max_4",
        F.coalesce(F.col("prev_prior_rows_3d"), F.lit(0)).alias("prev_prior_rows_3d_filled"),
    )
    .orderBy(F.col("rows_in_prior_rows_3d").desc(), "id")
    .limit(20)
)
