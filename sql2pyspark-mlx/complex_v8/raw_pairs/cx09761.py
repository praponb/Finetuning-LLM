from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_opened_on_month = Window.partitionBy(F.month("warehouse_opened_on"))
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_sku_weight_kg_ties = Window.orderBy(F.col("sku_weight_kg").desc())
w_moved_at_month_year_operator = Window.partitionBy(F.year("moved_at_month"), "operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_moved_at_2 = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at_month").asc_nulls_last(), "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_sku_weight_kg_dense_rank_sku = Window.partitionBy("sku_weight_kg_dense_rank", "sku_id").orderBy("sku_weight_kg_vs_warehouse_id_sku_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_weight_kg_dense_rank = Window.partitionBy("sku_weight_kg_dense_rank")
w_warehouse_opened_on_year_by = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("warehouse_opened_on")
w_warehouse_opened_on_year_by_2 = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("warehouse_opened_on", "id").rowsBetween(-2, -1)
w_sku_weight_kg_dense_rank_sku_2 = Window.partitionBy("sku_weight_kg_dense_rank", "sku_id").orderBy("sku_weight_kg_vs_warehouse_id_sku_id_avg", "id")
w_sku_weight_kg_dense_rank_sku_3 = Window.partitionBy("sku_weight_kg_dense_rank", "sku_id").orderBy("sku_weight_kg_vs_warehouse_id_sku_id_avg")
w_warehouse_opened_on_year_by_3 = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("qty") >= 189)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

combined_details_warehouse = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("B%")), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.moved_at_month",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

sequenced = (
    combined_details_warehouse
    .select(
        "*",
        F.count(F.lit(1)).over(w_warehouse_opened_on_month).alias("rows_in_warehouse_opened_on_month"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
        F.last("moved_at_month").over(w_sku_id_operator_by_moved_at).alias("last_moved_at_month"),
        F.dense_rank().over(w_all_by_sku_weight_kg_ties).alias("sku_weight_kg_dense_rank"),
        F.nth_value("qty", 2).over(w_moved_at_month_year_operator).alias("second_qty"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_opened_on_month), 2).alias("warehouse_opened_on_month_avg_sku_weight_kg"),
        F.lag("warehouse_opened_on").over(w_sku_id_operator_by_moved_at_2).alias("prev_warehouse_opened_on"),
        F.nth_value("warehouse_opened_on", 2).over(w_sku_id_operator_by_moved_at).alias("second_warehouse_opened_on"),
        F.lag("qty", 2, 0).over(w_sku_id_operator_by_moved_at_2).alias("prev2_qty"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_warehouse_id_sku_id), 2).alias("sku_weight_kg_vs_warehouse_id_sku_id_avg"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "moved_at_month",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "rows_in_warehouse_opened_on_month",
        "qty_share_of_sku_id",
        "sku_weight_kg_dense_rank",
        "warehouse_opened_on_month_avg_sku_weight_kg",
        "prev_warehouse_opened_on",
        "prev2_qty",
        F.last("moved_at_month", ignorenulls=True).over(w_sku_weight_kg_dense_rank_sku).alias("last_known_moved_at_month"),
        F.round(F.col("second_qty") - F.avg("second_qty").over(w_sku_weight_kg_dense_rank), 2).alias("second_qty_vs_sku_weight_kg_dense_rank_avg"),
        F.round(F.cume_dist().over(w_warehouse_opened_on_year_by), 3).alias("warehouse_opened_on_cume_dist"),
        F.max("qty").over(w_warehouse_opened_on_year_by_2).alias("qty_max_2_prior"),
        F.rank().over(w_sku_weight_kg_dense_rank_sku_2).alias("sku_weight_kg_vs_warehouse_id_sku_id_avg_rank"),
        F.round(F.cume_dist().over(w_sku_weight_kg_dense_rank_sku_3), 3).alias("sku_weight_kg_vs_warehouse_id_sku_id_avg_cume_di"),
        F.last("qty", ignorenulls=True).over(w_warehouse_opened_on_year_by_3).alias("last_known_qty"),
        F.size(F.collect_set("warehouse_id").over(w_sku_weight_kg_dense_rank)).alias("distinct_warehouse_id_in_sku_weight_kg_dense_ran"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "rows_in_warehouse_opened_on_month",
        "qty_share_of_sku_id",
        "sku_weight_kg_dense_rank",
        "warehouse_opened_on_month_avg_sku_weight_kg",
        "prev_warehouse_opened_on",
        "warehouse_opened_on_cume_dist",
        "qty_max_2_prior",
        "sku_weight_kg_vs_warehouse_id_sku_id_avg_rank",
        "sku_weight_kg_vs_warehouse_id_sku_id_avg_cume_di",
        "last_known_qty",
        "distinct_warehouse_id_in_sku_weight_kg_dense_ran",
        F.least(F.col("sku_weight_kg_dense_rank"), F.col("qty")).alias("least_sku_weight_kg_dense_rank_qty"),
    )
)
