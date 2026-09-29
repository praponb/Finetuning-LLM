from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_prior_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, -1)
w_sku_id_by_unit_price_moving = Window.partitionBy("sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price_ties = Window.partitionBy("sku_id").orderBy(F.col("unit_price").asc_nulls_last())
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_all_by_sku_unit_cost_avg_1_p = Window.orderBy(F.col("sku_unit_cost_avg_1_prior").asc_nulls_last(), "id")
w_warehouse_id_last_known_move = Window.partitionBy("warehouse_id", F.date_trunc("month", "last_known_moved_at")).orderBy("last_known_moved_at", "id")
w_all_by_sku_unit_cost_avg_1_p_2 = Window.orderBy(F.col("sku_unit_cost_avg_1_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_sku_category = Window.partitionBy(F.date_trunc("month", "moved_at"), "sku_category").orderBy(F.col("last_known_qty_plus_unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = (
    df_stock_moves
    .filter((F.col("unit_price").between(152.7, 292.3)) & (~F.col("move_type").isin("TRANSFER", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

combined_details_sku = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.qty_plus_unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

metrics = (
    combined_details_sku
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "sku_labels",
        "sku_category",
        "sku_unit_cost",
        F.count(F.lit(1)).over(w_moved_at_to_date).alias("rows_in_moved_at_to_date"),
        F.sum("qty_plus_unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_sum_qty_plus_unit_price"),
        F.lead("warehouse_capacity").over(w_all_by_unit_price).alias("next_warehouse_capacity"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_full).alias("second_unit_price"),
        F.round(F.avg("sku_unit_cost").over(w_all_by_unit_price_prior_1), 2).alias("sku_unit_cost_avg_1_prior"),
        F.round(F.avg("warehouse_capacity").over(w_sku_id_by_unit_price_moving), 2).alias("warehouse_capacity_avg_4"),
        F.last("moved_at", ignorenulls=True).over(w_sku_id_by_unit_price_running).alias("last_known_moved_at"),
        F.first("unit_price", ignorenulls=True).over(w_all_by_unit_price_running).alias("first_known_unit_price"),
        F.last("qty_plus_unit_price", ignorenulls=True).over(w_sku_id_warehouse_id_by_moved).alias("last_known_qty_plus_unit_price"),
        F.last("moved_at", ignorenulls=True).over(w_sku_id_warehouse_id_by_moved).alias("last_known_moved_at_2"),
        F.round(F.percent_rank().over(w_sku_id_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.sum("unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_sum_unit_price"),
        F.row_number().over(w_sku_id_warehouse_id_by_moved_2).alias("moved_at_seq"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_labels",
        "sku_unit_cost",
        "rows_in_moved_at_to_date",
        "moved_at_to_date_sum_qty_plus_unit_price",
        "next_warehouse_capacity",
        "sku_unit_cost_avg_1_prior",
        "warehouse_capacity_avg_4",
        "last_known_moved_at_2",
        "moved_at_to_date_sum_unit_price",
        F.ntile(10).over(w_all_by_sku_unit_cost_avg_1_p).alias("sku_unit_cost_avg_1_prior_decile"),
        F.lead("moved_at").over(w_warehouse_id_last_known_move).alias("next_moved_at"),
        F.last("sku_unit_cost_avg_1_prior").over(w_all_by_sku_unit_cost_avg_1_p_2).alias("last_sku_unit_cost_avg_1_prior"),
        F.nth_value("sku_unit_cost_avg_1_prior", 2).over(w_all_by_sku_unit_cost_avg_1_p_2).alias("second_sku_unit_cost_avg_1_prior"),
        F.first("first_known_unit_price").over(w_moved_at_month_sku_category).alias("first_first_known_unit_price"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_unit_cost",
        "rows_in_moved_at_to_date",
        "moved_at_to_date_sum_qty_plus_unit_price",
        "next_warehouse_capacity",
        "sku_unit_cost_avg_1_prior",
        "warehouse_capacity_avg_4",
        "last_known_moved_at_2",
        "moved_at_to_date_sum_unit_price",
        "sku_unit_cost_avg_1_prior_decile",
        "next_moved_at",
        "second_sku_unit_cost_avg_1_prior",
        "first_first_known_unit_price",
        (F.floor(F.col("next_warehouse_capacity") / 5000) * 5000).alias("next_warehouse_capacity_bucket"),
        F.coalesce(F.col("rows_in_moved_at_to_date"), F.lit(0)).alias("rows_in_moved_at_to_date_filled"),
    )
    .orderBy(
        "first_first_known_unit_price",
        F.col("rows_in_moved_at_to_date").desc(),
    )
)
