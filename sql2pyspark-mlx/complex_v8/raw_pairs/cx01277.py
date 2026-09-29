from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_moving_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "operator",
        F.lead("moved_at_date").over(w_all_by_unit_price).alias("next_moved_at_date"),
        F.sum("unit_price").over(w_all_by_unit_price_moving_6).alias("unit_price_sum_7"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.round(F.avg("unit_price").over(w_warehouse_id), 2).alias("warehouse_id_avg_unit_price"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_share_of_sku_id_operator"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.nth_value("moved_at_date", 2).over(w_all_by_unit_price_full).alias("second_moved_at_date"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("unit_price_rank") == 1)
    .drop("unit_price_rank")
)

result = (
    top_per_group
    .filter(F.col("distinct_sku_id_in_warehouse_id") <= 6)
    .select(
        "id",
        "operator",
        "next_moved_at_date",
        "unit_price_percent_rank",
        "warehouse_id_avg_unit_price",
        "unit_price_share_of_sku_id_operator",
        "unit_price_dense_rank",
        "distinct_sku_id_in_warehouse_id",
    )
    .orderBy("unit_price_dense_rank", "id")
    .limit(10)
)
