from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_move_type_by_un = Window.partitionBy("warehouse_id", "move_type").orderBy("unit_price", "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_sku_id_by_moved_at_prior_6 = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(-6, -1)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_by_moved_at_2 = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_n_move_type_is_not_null_sku = Window.partitionBy("n_move_type_is_not_null", "sku_id").orderBy("n_move_type_is_not_null", "sku_id")
w_n_move_type_is_not_null_sku_2 = Window.partitionBy("n_move_type_is_not_null", "sku_id").orderBy("n_move_type_is_not_null", "sku_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(6, 8)) & (F.col("qty") < 355))
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

windowed = (
    recent_moves
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_operator_warehouse_id)).alias("distinct_sku_id_in_operator_warehouse_id"),
        F.dense_rank().over(w_move_type_by_moved_at).alias("moved_at_dense_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_move_type_by_unit_price)).alias("unit_price_change"),
        F.lag("qty", 3).over(w_warehouse_id_move_type_by_un).alias("prev3_qty"),
        F.count(F.lit(1)).over(w_sku_id_operator).alias("rows_in_sku_id_operator"),
        F.sum("unit_price").over(w_sku_id_by_moved_at_prior_6).alias("unit_price_sum_6_prior"),
        F.lead("qty").over(w_sku_id_by_moved_at).alias("next_qty"),
        F.lead("qty").over(w_sku_id_by_moved_at_2).alias("next_qty_2"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("moved_at_dense_rank") <= 5)
    .drop("moved_at_dense_rank")
)

totals = (
    top_per_group
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("n_move_type_is_not_null"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.round(F.avg("next_qty_2"), 2).alias("avg_next_qty_2"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.lag("n_move_type_is_not_null", 3).over(w_n_move_type_is_not_null_sku).alias("prev3_n_move_type_is_not_null"),
        F.lead("n_move_type_is_not_null").over(w_n_move_type_is_not_null_sku).alias("next_n_move_type_is_not_null"),
        F.count(F.lit(1)).over(w_n_move_type_is_not_null_sku_2).alias("running_count"),
        F.rank().over(w_n_move_type_is_not_null_sku).alias("n_move_type_is_not_null_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("next_n_move_type_is_not_null").between(2, 6))
    .select(
        "sku_id",
        "operator_list",
        "avg_next_qty_2",
        "prev3_n_move_type_is_not_null",
        "next_n_move_type_is_not_null",
        "running_count",
        "n_move_type_is_not_null_rank",
        F.greatest(F.col("avg_next_qty_2"), F.col("n_move_type_is_not_null")).alias("greatest_avg_next_qty_2_n_move_type_is_not_null"),
    )
    .orderBy("greatest_avg_next_qty_2_n_move_type_is_not_null", "sku_id")
    .limit(10)
)
