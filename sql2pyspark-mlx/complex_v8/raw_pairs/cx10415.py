from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_unit_price_moving = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_unit_price_ties = Window.partitionBy("move_type").orderBy(F.col("unit_price").asc_nulls_last())
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_moves = (
    df_stock_moves
    .filter(F.col("qty") > 169)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

with_trends = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        F.sum("unit_price").over(w_sku_id_by_unit_price_moving).alias("unit_price_sum_4"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.sum("unit_price").over(w_warehouse_id).alias("warehouse_id_sum_unit_price"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_unit_price_full).alias("second_moved_at"),
        F.sum("unit_price").over(w_move_type).alias("move_type_sum_unit_price"),
        F.rank().over(w_move_type_by_unit_price_ties).alias("unit_price_rank"),
        F.last("unit_price", ignorenulls=True).over(w_sku_id_by_unit_price_running).alias("last_known_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
    )
)

grouped = (
    with_trends
    .filter(F.col("distinct_warehouse_id_in_move_type").between(4, 9))
    .groupBy("warehouse_id", "sku_id")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.sum(F.when(F.col("move_type") == "OUTBOUND", F.col("last_known_unit_price"))).alias("last_known_unit_price_move_type_outbound"),
        F.round(F.avg("unit_price_sum_4"), 2).alias("avg_unit_price_sum_4"),
        F.count("*").alias("n_rows"),
        F.min("second_moved_at").alias("first_second_moved_at"),
        F.round(F.avg("distinct_sku_id_in_move_type"), 2).alias("avg_distinct_sku_id_in_move_type"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    grouped
    .filter(F.col("warehouse_id") >= 5)
    .select(
        "warehouse_id",
        "n_rows",
        "first_second_moved_at",
        "avg_distinct_sku_id_in_move_type",
        F.array_join(F.array_sort(F.col("move_type_list")), ",").alias("move_type_list_text"),
    )
    .orderBy(F.col("warehouse_id").desc())
)
