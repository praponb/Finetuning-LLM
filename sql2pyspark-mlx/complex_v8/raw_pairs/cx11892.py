from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_move_type_operator_by_unit_p = Window.partitionBy("move_type", "operator").orderBy("unit_price")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_all_by_qty_ties = Window.orderBy(F.col("qty").asc_nulls_last())
w_sku_id = Window.partitionBy("sku_id")
w_all_by_greatest_qty_unit_pri = Window.orderBy(F.col("greatest_qty_unit_price").desc(), "id")
w_move_type_operator_by_unit_p_2 = Window.partitionBy("move_type", "operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_greatest_qty_unit_pri_2 = Window.orderBy(F.col("greatest_qty_unit_price").desc())
w_moved_at_month_warehouse_id = Window.partitionBy(F.month("moved_at"), "warehouse_id").orderBy("qty", "id")
w_sku_id_operator_by_qty_terci = Window.partitionBy("sku_id", "operator").orderBy("qty_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_tercile_bucket = Window.partitionBy("qty_tercile_bucket")

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w_all_by_qty).alias("qty_decile"),
        F.round(F.percent_rank().over(w_move_type_operator_by_unit_p), 3).alias("unit_price_percent_rank"),
        F.max("unit_price").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_max_unit_price"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.dense_rank().over(w_all_by_greatest_qty_unit_pri).alias("greatest_qty_unit_price_dense_rank"),
        F.last("qty").over(w_move_type_operator_by_unit_p_2).alias("last_qty"),
        F.dense_rank().over(w_all_by_greatest_qty_unit_pri_2).alias("greatest_qty_unit_price_dense_rank_2"),
        F.lag("unit_price").over(w_moved_at_month_warehouse_id).alias("prev_unit_price"),
        F.ntile(3).over(w_moved_at_month_warehouse_id).alias("qty_tercile"),
    )
)

scored = with_trends.withColumn("qty_tercile_bucket", F.floor(F.col("qty_tercile") / 2) * 2)

ranked = (
    scored
    .select(
        "id",
        "sku_id",
        "qty",
        "unit_price",
        "qty_decile",
        "unit_price_percent_rank",
        "warehouse_id_move_type_max_unit_price",
        "rows_in_sku_id",
        "greatest_qty_unit_price_dense_rank",
        "last_qty",
        "greatest_qty_unit_price_dense_rank_2",
        "prev_unit_price",
        "qty_tercile_bucket",
        F.last("prev_unit_price").over(w_sku_id_operator_by_qty_terci).alias("last_prev_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_qty_tercile_bucket)).alias("distinct_warehouse_id_in_qty_tercile_bucket"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "qty",
        "unit_price",
        "qty_decile",
        "unit_price_percent_rank",
        "warehouse_id_move_type_max_unit_price",
        "greatest_qty_unit_price_dense_rank",
        "last_qty",
        "prev_unit_price",
        "last_prev_unit_price",
        "distinct_warehouse_id_in_qty_tercile_bucket",
        F.greatest(F.col("qty_decile"), F.col("last_prev_unit_price")).alias("greatest_qty_decile_last_prev_unit_price"),
    )
    .orderBy(
        F.col("warehouse_id_move_type_max_unit_price").asc(),
        F.col("unit_price_percent_rank").desc(),
    )
)
