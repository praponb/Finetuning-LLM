from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_qty_f = Window.partitionBy("warehouse_id", "sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy("unit_price_minus_qty")
w_warehouse_id_by_unit_price_m = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_minus_qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_sku_id_by_qty = Window.partitionBy("warehouse_id", "sku_id").orderBy("qty", "id")
w_warehouse_id_moved_at_month_2 = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy("unit_price_minus_qty", "id").rowsBetween(-6, -1)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_moved_at_month_by_qty = Window.partitionBy(F.month("moved_at")).orderBy(F.col("qty").desc(), "id")
w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc())
w_sku_id_by_moved_at_full = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_quartile_by_qty_percent = Window.partitionBy("qty_quartile").orderBy("qty_percent_rank", "id").rowsBetween(-1, -1)
w_unit_price_minus_qty_dense_r = Window.partitionBy("unit_price_minus_qty_dense_rank", "distinct_sku_id_in_warehouse_id").orderBy("sku_id_warehouse_id_sum_unit_price", "id")
w_qty_quartile_by_qty_percent_2 = Window.partitionBy("qty_quartile").orderBy("qty_percent_rank", "id").rowsBetween(-2, Window.currentRow)
w_moved_at_dense_rank_sku_id = Window.partitionBy("moved_at_dense_rank", "sku_id")
w_qty_quartile_by_qty_percent_3 = Window.partitionBy("qty_quartile").orderBy("qty_percent_rank", "id")
w_qty_quartile_moved_at_month = Window.partitionBy("qty_quartile", F.date_trunc("month", "moved_at")).orderBy(F.col("unit_price_minus_qty_dense_rank").desc_nulls_first(), "id")
w_all_by_row_count_prior_2 = Window.orderBy(F.col("row_count").desc(), "moved_at_month").rowsBetween(-2, -1)
w_all_by_row_count_prior_3 = Window.orderBy(F.col("row_count").desc(), "moved_at_month").rowsBetween(-3, -1)
w_all_by_row_count_full = Window.orderBy(F.col("row_count").desc(), "moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_row_count_ties = Window.orderBy(F.col("row_count").desc())

scoped = (
    df_stock_moves
    .filter(F.col("qty").between(124, 422))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.nth_value("unit_price_minus_qty", 2).over(w_warehouse_id_sku_id_by_qty_f).alias("second_unit_price_minus_qty"),
        F.round(F.avg("unit_price_minus_qty").over(w_sku_id_warehouse_id), 2).alias("sku_id_warehouse_id_avg_unit_price_minus_qty"),
        F.round(F.col("unit_price_minus_qty") - F.avg("unit_price_minus_qty").over(w_sku_id_warehouse_id), 2).alias("unit_price_minus_qty_vs_sku_id_warehouse_id_avg"),
        F.dense_rank().over(w_warehouse_id_moved_at_month).alias("unit_price_minus_qty_dense_rank"),
        F.first("qty", ignorenulls=True).over(w_warehouse_id_by_unit_price_m).alias("first_known_qty"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_by_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.ntile(4).over(w_warehouse_id_sku_id_by_qty).alias("qty_quartile"),
        F.sum("unit_price_minus_qty").over(w_warehouse_id_moved_at_month_2).alias("unit_price_minus_qty_sum_6_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.sum("unit_price").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_sum_unit_price"),
        F.lag("unit_price", 2, 0).over(w_moved_at_month_by_qty).alias("prev2_unit_price"),
        F.dense_rank().over(w_sku_id_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.max("unit_price").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_max_unit_price"),
        F.round(F.percent_rank().over(w_moved_at_month_by_qty), 3).alias("qty_percent_rank"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_moved_at_full).alias("second_moved_at"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "unit_price_minus_qty",
        "second_unit_price_minus_qty",
        "sku_id_warehouse_id_avg_unit_price_minus_qty",
        "unit_price_minus_qty_vs_sku_id_warehouse_id_avg",
        "unit_price_minus_qty_dense_rank",
        "minutes_since_prev_moved_at",
        "unit_price_minus_qty_sum_6_prior",
        "sku_id_warehouse_id_sum_unit_price",
        "prev2_unit_price",
        "moved_at_dense_rank",
        "sku_id_warehouse_id_max_unit_price",
        "qty_percent_rank",
        "second_moved_at",
        "distinct_sku_id_in_warehouse_id",
        F.round(F.avg("qty_percent_rank").over(w_qty_quartile_by_qty_percent), 2).alias("qty_percent_rank_avg_1_prior"),
        F.lag("unit_price").over(w_unit_price_minus_qty_dense_r).alias("prev_unit_price"),
        F.round(F.avg("qty_percent_rank").over(w_qty_quartile_by_qty_percent_2), 2).alias("qty_percent_rank_avg_3"),
        F.count(F.lit(1)).over(w_moved_at_dense_rank_sku_id).alias("rows_in_moved_at_dense_rank_sku_id"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_dense_rank_sku_id)).alias("distinct_warehouse_id_in_moved_at_dense_rank_sku"),
        F.round(F.cume_dist().over(w_qty_quartile_by_qty_percent_3), 3).alias("qty_percent_rank_cume_dist"),
        F.lag("first_known_qty", 3).over(w_qty_quartile_moved_at_month).alias("prev3_first_known_qty"),
    )
)

summary = (
    ranked_2
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"))
    .agg(
        F.min("second_moved_at").alias("first_second_moved_at"),
        F.count("*").alias("row_count"),
    )
)

metrics = (
    summary
    .select(
        "moved_at_month",
        "row_count",
        F.max("row_count").over(w_all_by_row_count_prior_2).alias("row_count_max_2_prior"),
        F.sum("row_count").over(w_all_by_row_count_prior_3).alias("row_count_sum_3_prior"),
        F.last("moved_at_month").over(w_all_by_row_count_full).alias("last_moved_at_month"),
        F.round(F.cume_dist().over(w_all_by_row_count_ties), 3).alias("row_count_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "moved_at_month",
        "row_count",
        "row_count_sum_3_prior",
        "row_count_cume_dist",
        F.date_trunc("quarter", F.col("last_moved_at_month")).alias("last_moved_at_month_quarter"),
        F.to_date(F.col("last_moved_at_month")).alias("last_moved_at_month_date"),
    )
    .orderBy("moved_at_month")
)
