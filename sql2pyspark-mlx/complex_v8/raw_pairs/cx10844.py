from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_all_by_sku_unit_cost = Window.orderBy("sku_unit_cost", "id")
w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price_distance").asc_nulls_last(), "id")
w_operator_sku_id_by_unit_pric_2 = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category_sku_id_by_unit = Window.partitionBy("sku_category", "sku_id").orderBy("unit_price_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category_sku_id_by_unit_2 = Window.partitionBy("sku_category", "sku_id").orderBy("unit_price_distance", "id")
w_all_by_sku_unit_cost_running = Window.orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_unit_price_distan = Window.orderBy("max_unit_price_distance_so_far", "id")
w_distinct_sku_category_in_war = Window.partitionBy("distinct_sku_category_in_warehouse_id_operator", "sku_category")
w_all_by_max_unit_price_distan_2 = Window.orderBy("max_unit_price_distance_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_unit_price_distan_3 = Window.orderBy("max_unit_price_distance_so_far", "id").rowsBetween(-2, -1)

cleaned_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.abs(F.col("unit_price") - 298.9).alias("unit_price_distance"),
    )
)

enriched = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.move_type_operator_label",
        "s.unit_price_distance",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        (F.col("sku_unit_cost") - F.lag("sku_unit_cost").over(w_all_by_sku_unit_cost)).alias("sku_unit_cost_change"),
        F.size(F.collect_set("sku_category").over(w_warehouse_id_operator)).alias("distinct_sku_category_in_warehouse_id_operator"),
        F.lag("sku_unit_cost").over(w_operator_sku_id_by_unit_pric).alias("prev_sku_unit_cost"),
        F.min("unit_price_distance").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_unit_price_distance"),
        F.nth_value("unit_price_distance", 2).over(w_operator_sku_id_by_unit_pric_2).alias("second_unit_price_distance"),
        F.first("unit_price_distance").over(w_sku_category_sku_id_by_unit).alias("first_unit_price_distance"),
        F.lag("qty", 3, 0).over(w_sku_category_sku_id_by_unit_2).alias("prev3_qty"),
        F.row_number().over(w_all_by_sku_unit_cost).alias("rn"),
        F.max("unit_price_distance").over(w_all_by_sku_unit_cost_running).alias("max_unit_price_distance_so_far"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "operator",
        "move_type_operator_label",
        "unit_price_distance",
        "sku_category",
        "sku_labels",
        "sku_unit_cost",
        "rows_in_warehouse_id_operator",
        "distinct_sku_category_in_warehouse_id_operator",
        "prev_sku_unit_cost",
        "second_unit_price_distance",
        "first_unit_price_distance",
        "prev3_qty",
        "rn",
        F.row_number().over(w_all_by_max_unit_price_distan).alias("rn_2"),
        F.lag("sku_unit_cost_change").over(w_all_by_max_unit_price_distan).alias("prev_sku_unit_cost_change"),
        F.round(F.col("first_unit_price_distance") / F.nullif(F.sum("first_unit_price_distance").over(w_distinct_sku_category_in_war), F.lit(0)), 4).alias("first_unit_price_distance_share_of_distinct_sku_"),
        F.last("prev_sku_unit_cost", ignorenulls=True).over(w_all_by_max_unit_price_distan_2).alias("last_known_prev_sku_unit_cost"),
        F.round(F.avg("sku_unit_cost_change").over(w_all_by_max_unit_price_distan_3), 2).alias("sku_unit_cost_change_avg_2_prior"),
        F.size(F.collect_set("operator").over(w_distinct_sku_category_in_war)).alias("distinct_operator_in_distinct_sku_category_in_wa"),
        F.count(F.lit(1)).over(w_distinct_sku_category_in_war).alias("rows_in_distinct_sku_category_in_warehouse_id_op"),
    )
)

totals = (
    windowed
    .filter(F.col("second_unit_price_distance") < 300.0)
    .groupBy("sku_category", "rn")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum("distinct_operator_in_distinct_sku_category_in_wa").alias("total_distinct_operator_in_distinct_sku_category"),
    )
)

result = (
    totals
    .select("sku_category", "rn")
    .orderBy(F.col("rn").asc_nulls_last())
)
