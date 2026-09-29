from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_sku_is_active_warehouse_id = Window.partitionBy("sku_is_active", "warehouse_id")
w_operator_sku_id_by_sku_unit = Window.partitionBy("operator", "sku_id").orderBy("sku_unit_cost", "id")
w_operator_by_sku_unit_cost_fu = Window.partitionBy("operator").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_prior_1 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, -1)

recent_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 189.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

combined_details_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    combined_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "sku_is_active",
        "sku_labels",
        "sku_unit_cost",
        F.sum("sku_unit_cost").over(w_operator).alias("operator_sum_sku_unit_cost"),
        F.count(F.lit(1)).over(w_sku_is_active_warehouse_id).alias("rows_in_sku_is_active_warehouse_id"),
        F.row_number().over(w_operator_sku_id_by_sku_unit).alias("sku_unit_cost_seq"),
        F.nth_value("moved_at", 2).over(w_operator_by_sku_unit_cost_fu).alias("second_moved_at"),
        F.sum("sku_unit_cost").over(w_all_by_moved_at_prior_1).alias("sku_unit_cost_sum_1_prior"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_operator), F.lit(0)), 4).alias("sku_unit_cost_share_of_operator"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_sku_is_active_warehouse_id), 2).alias("sku_unit_cost_vs_sku_is_active_warehouse_id_avg"),
        F.max("sku_unit_cost").over(w_sku_is_active_warehouse_id).alias("sku_is_active_warehouse_id_max_sku_unit_cost"),
        F.size(F.collect_set("operator").over(w_sku_is_active_warehouse_id)).alias("distinct_operator_in_sku_is_active_warehouse_id"),
    )
)

result = (
    metrics
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "sku_id",
        "operator",
        "sku_is_active",
        "sku_labels",
        "rows_in_sku_is_active_warehouse_id",
        "sku_unit_cost_seq",
        "sku_unit_cost_sum_1_prior",
        "sku_unit_cost_share_of_operator",
        "sku_unit_cost_vs_sku_is_active_warehouse_id_avg",
        "sku_is_active_warehouse_id_max_sku_unit_cost",
        (F.floor(F.col("sku_unit_cost") / 50) * 50).alias("sku_unit_cost_bucket"),
    )
    .orderBy(F.col("sku_unit_cost_bucket").desc())
)
