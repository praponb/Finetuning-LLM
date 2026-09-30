from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty = Window.orderBy("qty", "id")
w_sku_id = Window.partitionBy("sku_id")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy(F.col("qty").desc())
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc())
w_warehouse_city_sku_id_by_mov = Window.partitionBy("warehouse_city", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_city = Window.partitionBy("sku_id", "warehouse_city")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_city_sku_id_by_mov_2 = Window.partitionBy("warehouse_city", "sku_id").orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_n_operator_is_not_null_row_c = Window.partitionBy("n_operator_is_not_null", "row_count").orderBy("row_count", "moved_at_dense_rank", "next_moved_at_ym")
w_moved_at_dense_rank_row_coun = Window.partitionBy("moved_at_dense_rank", "row_count").orderBy("n_operator_is_not_null", "moved_at_dense_rank", "next_moved_at_ym").rowsBetween(-3, -1)

recent = (
    df_stock_moves
    .filter((F.col("unit_price") <= 290.4) & (F.col("warehouse_id") <= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

joined = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "sku_weight_kg",
        "sku_labels",
        "warehouse_city",
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.lead("moved_at").over(w_all_by_qty).alias("next_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.rank().over(w_operator_by_qty_ties).alias("qty_rank_2"),
        F.sum("sku_weight_kg").over(w_all_by_qty_running).alias("running_sku_weight_kg"),
        F.nth_value("unit_price", 2).over(w_operator_by_qty_full).alias("second_unit_price"),
        F.first("qty", ignorenulls=True).over(w_warehouse_id_by_moved_at_run).alias("first_known_qty"),
        F.dense_rank().over(w_warehouse_id_by_moved_at_tie).alias("moved_at_dense_rank"),
        F.nth_value("sku_weight_kg", 2).over(w_warehouse_city_sku_id_by_mov).alias("second_sku_weight_kg"),
        F.last("moved_at").over(w_all_by_unit_price_full).alias("last_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_warehouse_city), F.lit(0)), 4).alias("unit_price_share_of_sku_id_warehouse_city"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.sum("qty").over(w_warehouse_city_sku_id_by_mov_2).alias("qty_sum_7"),
        F.max("sku_weight_kg").over(w_warehouse_city).alias("warehouse_city_max_sku_weight_kg"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.max("unit_price").over(w_all_by_qty_running).alias("max_unit_price_so_far"),
    )
)

totals = (
    metrics
    .filter(F.col("sku_weight_kg") >= 13.6)
    .groupBy(
        "moved_at_dense_rank",
        F.date_format("next_moved_at", "yyyy-MM").alias("next_moved_at_ym"),
    )
    .agg(
        F.sum("qty_sum_7").alias("total_qty_sum_7"),
        F.sum(F.when(F.col("operator").isNotNull(), 1).otherwise(0)).alias("n_operator_is_not_null"),
        F.percentile_approx("sku_weight_kg", 0.5).alias("median_sku_weight_kg"),
        F.max("moved_at").alias("last_moved_at_2"),
        F.count("*").alias("row_count"),
    )
)

metrics_2 = (
    totals
    .select(
        "*",
        (F.col("total_qty_sum_7") - F.lag("total_qty_sum_7").over(w_n_operator_is_not_null_row_c)).alias("total_qty_sum_7_change"),
        F.round(F.cume_dist().over(w_n_operator_is_not_null_row_c), 3).alias("row_count_cume_dist"),
        F.round(F.avg("row_count").over(w_moved_at_dense_rank_row_coun), 2).alias("row_count_avg_3_prior"),
        F.max("median_sku_weight_kg").over(w_moved_at_dense_rank_row_coun).alias("median_sku_weight_kg_max_3_prior"),
    )
)

result = (
    metrics_2
    .filter(F.col("next_moved_at_ym").isNotNull())
    .select(
        "moved_at_dense_rank",
        "next_moved_at_ym",
        "total_qty_sum_7",
        "median_sku_weight_kg",
        "last_moved_at_2",
        "row_count",
        "total_qty_sum_7_change",
        "row_count_cume_dist",
        "row_count_avg_3_prior",
        "median_sku_weight_kg_max_3_prior",
        F.coalesce(F.col("row_count_cume_dist"), F.lit(0.0)).alias("row_count_cume_dist_filled"),
        F.date_trunc("month", F.col("last_moved_at_2")).alias("last_moved_at_2_month"),
    )
    .orderBy(F.col("row_count_cume_dist").desc())
)
