from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_move_type_group_by_moved_at = Window.partitionBy("move_type_group").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_move_type_group_by_mo = Window.partitionBy("sku_id", "move_type_group").orderBy("moved_at", "id")
w_all_by_moved_at_prior_2 = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_sku_id_move_type_group_by_mo_2 = Window.partitionBy("sku_id", "move_type_group").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_qty_sum_3_pr = Window.partitionBy("warehouse_id").orderBy("qty_sum_3_prior", "id").rowsBetween(-3, -1)
w_all_by_moved_at_full_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_qty_sum_3_pr_2 = Window.partitionBy("warehouse_id").orderBy("qty_sum_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_sku_id_in_warehouse = Window.partitionBy("distinct_sku_id_in_warehouse_id", "rn")
w_warehouse_id_by_qty_sum_3_pr_3 = Window.partitionBy("warehouse_id").orderBy("qty_sum_3_prior", "id")

scoped_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        F.when(F.col("move_type").isin("INBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

joined_sku = (
    scoped_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_ref",
        "s.move_type_group",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

with_windows = (
    joined_sku
    .select(
        "*",
        F.dense_rank().over(w_warehouse_id_by_qty).alias("qty_dense_rank"),
        F.sum("qty").over(w_move_type_group_by_moved_at).alias("qty_sum_3_prior"),
        F.nth_value("sku_unit_cost", 2).over(w_warehouse_id_by_qty_full).alias("second_sku_unit_cost"),
        F.nth_value("unit_price", 2).over(w_all_by_moved_at_full).alias("second_unit_price"),
        F.ntile(4).over(w_all_by_moved_at).alias("moved_at_quartile"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.round(F.avg("unit_price").over(w_warehouse_id), 2).alias("warehouse_id_avg_unit_price"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_move_type_group_by_mo))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.max("qty").over(w_all_by_moved_at_prior_2).alias("qty_max_2_prior"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.row_number().over(w_warehouse_id_by_qty).alias("rn"),
        F.nth_value("unit_price", 2).over(w_sku_id_move_type_group_by_mo_2).alias("second_unit_price_2"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_warehouse_id), F.lit(0)), 4).alias("sku_unit_cost_share_of_warehouse_id"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.concat_ws(" / ", F.col("move_type_group"), F.col("move_type_ref")).alias("move_type_group_move_type_ref_label"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.avg("qty_max_2_prior").over(w_warehouse_id_by_qty_sum_3_pr), 2).alias("qty_max_2_prior_avg_3_prior"),
        F.nth_value("operator_len", 2).over(w_all_by_moved_at_full_2).alias("second_operator_len"),
        F.nth_value("unit_price", 2).over(w_warehouse_id_by_qty_sum_3_pr_2).alias("second_unit_price_3"),
        F.size(F.collect_set("move_type_group").over(w_distinct_sku_id_in_warehouse)).alias("distinct_move_type_group_in_distinct_sku_id_in_w"),
        F.lead("warehouse_id_avg_unit_price", 2).over(w_warehouse_id_by_qty_sum_3_pr_3).alias("next2_warehouse_id_avg_unit_price"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_ref",
        "move_type_group",
        "sku_unit_cost",
        "qty_dense_rank",
        "qty_sum_3_prior",
        "second_unit_price",
        "moved_at_quartile",
        "qty_share_of_warehouse_id",
        "warehouse_id_avg_unit_price",
        "distinct_sku_id_in_warehouse_id",
        "qty_max_2_prior",
        "sku_unit_cost_share_of_warehouse_id",
        "operator_len",
        "move_type_group_move_type_ref_label",
        "qty_max_2_prior_avg_3_prior",
        "second_operator_len",
        "second_unit_price_3",
        F.length(F.trim(F.col("move_type_group"))).alias("move_type_group_len"),
    )
    .orderBy(
        F.col("moved_at_quartile").asc_nulls_last(),
        F.col("moved_at").desc_nulls_first(),
    )
)
