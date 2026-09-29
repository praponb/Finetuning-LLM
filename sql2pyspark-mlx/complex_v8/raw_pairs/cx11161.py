from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_sku_u = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_sku_id_by_sku_u_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("sku_unit_cost").desc(), "id")
w_warehouse_id_sku_id_by_sku_u_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("sku_unit_cost").desc())
w_all_by_moved_at_date = Window.orderBy("moved_at_date", "id")
w_warehouse_id_sku_id_by_sku_u_4 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-2, -1)
w_all_by_qty_avg_2_prior = Window.orderBy("qty_avg_2_prior", "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_last_warehouse_opened_on_yea = Window.partitionBy(F.year("last_warehouse_opened_on"))
w_all_by_qty_avg_2_prior_full = Window.orderBy("qty_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_avg_2_prior_ties = Window.orderBy("qty_avg_2_prior")

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

enriched = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.moved_at_date",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.last("warehouse_opened_on").over(w_warehouse_id_sku_id_by_sku_u).alias("last_warehouse_opened_on"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_move_type), F.lit(0)), 4).alias("sku_unit_cost_share_of_move_type"),
        F.sum("sku_unit_cost").over(w_move_type).alias("move_type_sum_sku_unit_cost"),
        F.row_number().over(w_warehouse_id_sku_id_by_sku_u_2).alias("rn"),
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_sku_u_3), 3).alias("sku_unit_cost_cume_dist"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type), F.lit(0)), 4).alias("qty_share_of_move_type"),
        F.lag("qty").over(w_all_by_moved_at_date).alias("prev_qty"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_id_by_sku_u_4), 2).alias("qty_avg_2_prior"),
    )
)

leaders = (
    metrics
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    leaders
    .select(
        "*",
        F.datediff(F.col("last_warehouse_opened_on"), F.lag("last_warehouse_opened_on").over(w_all_by_qty_avg_2_prior)).alias("days_since_prev_last_warehouse_opened_on"),
        F.row_number().over(w_all_by_moved_at).alias("moved_at_seq"),
        F.round(F.col("rows_in_move_type") / F.nullif(F.sum("rows_in_move_type").over(w_last_warehouse_opened_on_yea), F.lit(0)), 4).alias("rows_in_move_type_share_of_last_warehouse_opened"),
        F.last("rows_in_move_type").over(w_all_by_qty_avg_2_prior_full).alias("last_rows_in_move_type"),
        F.ntile(3).over(w_all_by_qty_avg_2_prior).alias("qty_avg_2_prior_tercile"),
        F.round(F.percent_rank().over(w_all_by_qty_avg_2_prior_ties), 3).alias("qty_avg_2_prior_percent_rank"),
        F.max("sku_unit_cost").over(w_last_warehouse_opened_on_yea).alias("last_warehouse_opened_on_year_max_sku_unit_cost"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("last_rows_in_move_type") >= 5)
    .groupBy(
        F.date_trunc("month", "last_warehouse_opened_on").alias("last_warehouse_opened_on_month"),
        "moved_at_seq",
    )
    .agg(
        F.min("moved_at_date").alias("first_moved_at_date"),
        F.round(100 * F.count(F.when(~F.col("move_type").isin("TRANSFER", "ADJUST"), 1)) / F.count("*"), 1).alias("pct_move_type_not_in_transfer_ad"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum("last_rows_in_move_type").alias("sum_last_rows_in_move_type_check"),
    )
    .filter(F.col("sum_last_rows_in_move_type_check") > 4)
    .drop("sum_last_rows_in_move_type_check")
)

result = (
    rollup_base
    .filter(F.col("moved_at_seq") <= 6)
    .select(
        "last_warehouse_opened_on_month",
        "first_moved_at_date",
        "pct_move_type_not_in_transfer_ad",
        "distinct_ids",
    )
    .orderBy(F.col("last_warehouse_opened_on_month").asc())
)
