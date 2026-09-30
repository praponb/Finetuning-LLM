from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id").rowsBetween(-1, -1)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_operator = Window.partitionBy("operator")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_category_by_mo = Window.partitionBy("move_type", "sku_category").orderBy("moved_at")
w_distinct_move_type_in_wareho = Window.partitionBy("distinct_move_type_in_warehouse_id_sku_id", "sku_id")
w_move_type_sku_category_by_mo_2 = Window.partitionBy("move_type", "sku_category").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_warehouse_id_sku_id_a = Window.orderBy(F.col("warehouse_id_sku_id_avg_qty").desc(), "id")

base = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 3)) & (F.col("warehouse_id") <= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_details_sku = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    with_details_sku
    .select(
        "*",
        F.max("sku_unit_cost").over(w_sku_id_move_type_by_moved_at).alias("sku_unit_cost_max_1_prior"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_sku_id)).alias("distinct_move_type_in_warehouse_id_sku_id"),
        F.lag("unit_price", 2).over(w_all_by_unit_price).alias("prev2_unit_price"),
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator), F.lit(0)), 4).alias("qty_share_of_operator"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.first("qty", ignorenulls=True).over(w_all_by_unit_price_running).alias("first_known_qty"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_move_type_sku_category_by_mo).alias("moved_at_dense_rank"),
        F.round(F.col("prev2_unit_price") - F.avg("prev2_unit_price").over(w_distinct_move_type_in_wareho), 2).alias("prev2_unit_price_vs_distinct_move_type_in_wareho"),
        F.round(F.col("sku_unit_cost_max_1_prior") / F.nullif(F.sum("sku_unit_cost_max_1_prior").over(w_distinct_move_type_in_wareho), F.lit(0)), 4).alias("sku_unit_cost_max_1_prior_share_of_distinct_move"),
        F.round(F.col("prev2_unit_price") / F.nullif(F.sum("prev2_unit_price").over(w_distinct_move_type_in_wareho), F.lit(0)), 4).alias("prev2_unit_price_share_of_distinct_move_type_in_"),
        F.size(F.collect_set("operator").over(w_distinct_move_type_in_wareho)).alias("distinct_operator_in_distinct_move_type_in_wareh"),
        F.min("sku_unit_cost_max_1_prior").over(w_move_type_sku_category_by_mo_2).alias("min_sku_unit_cost_max_1_prior_so_far"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_warehouse_id_sku_id_a)).alias("unit_price_change"),
        F.size(F.collect_set("warehouse_id").over(w_distinct_move_type_in_wareho)).alias("distinct_warehouse_id_in_distinct_move_type_in_w"),
    )
)

result = (
    metrics
    .filter(F.col("unit_price") >= 152.7)
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "operator_move_type_label",
        "sku_unit_cost",
        "sku_labels",
        "sku_category",
        "sku_unit_cost_max_1_prior",
        "prev2_unit_price",
        "second_qty",
        "first_known_qty",
        "moved_at_dense_rank",
        "prev2_unit_price_vs_distinct_move_type_in_wareho",
        "sku_unit_cost_max_1_prior_share_of_distinct_move",
        "prev2_unit_price_share_of_distinct_move_type_in_",
        "distinct_operator_in_distinct_move_type_in_wareh",
        "min_sku_unit_cost_max_1_prior_so_far",
        "distinct_warehouse_id_in_distinct_move_type_in_w",
        F.when(F.col("operator").isin("op_chan", "op_dew"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("moved_at_dense_rank").desc())
)
