from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_moved_at_year_by_move = Window.partitionBy("sku_id", F.year("moved_at")).orderBy("moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_unit_price = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("unit_price", "id")
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price")
w_moved_at_year = Window.partitionBy(F.year("moved_at"))
w_operator = Window.partitionBy("operator")
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_moved_at_year_by_move_2 = Window.partitionBy("sku_id", F.year("moved_at")).orderBy("moved_at_month", "id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_all_by_second_unit_price_pri = Window.orderBy(F.col("second_unit_price").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_unit_price_rank_distinct_sku = Window.partitionBy("unit_price_rank", "distinct_sku_id_in_operator").orderBy(F.col("first_unit_price").desc())
w_unit_price_dense_rank = Window.partitionBy("unit_price_dense_rank")

recent_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "OUTBOUND", "TRANSFER")) & (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
)

windowed = (
    recent_moves
    .select(
        "*",
        F.first("unit_price").over(w_sku_id_moved_at_year_by_move).alias("first_unit_price"),
        F.lead("moved_at", 3).over(w_moved_at_month_by_unit_price).alias("next3_moved_at"),
        F.dense_rank().over(w_move_type_warehouse_id_by_un).alias("unit_price_dense_rank"),
        F.round(F.avg("qty").over(w_moved_at_year), 2).alias("moved_at_year_avg_qty"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_full).alias("second_unit_price"),
        (F.col("qty") - F.lag("qty").over(w_sku_id_moved_at_year_by_move_2)).alias("qty_change"),
        F.rank().over(w_all_by_unit_price).alias("unit_price_rank"),
    )
)

top_rows = windowed.filter(F.col("unit_price_rank") == 1)

with_trends = (
    top_rows
    .select(
        "id",
        "moved_at",
        "unit_price",
        "moved_at_month",
        "first_unit_price",
        "next3_moved_at",
        "unit_price_dense_rank",
        "moved_at_year_avg_qty",
        "distinct_sku_id_in_operator",
        "second_unit_price",
        "unit_price_rank",
        F.max("first_unit_price").over(w_all_by_second_unit_price_pri).alias("first_unit_price_max_3_prior"),
        F.rank().over(w_unit_price_rank_distinct_sku).alias("first_unit_price_rank"),
        F.sum("unit_price_rank").over(w_unit_price_dense_rank).alias("unit_price_dense_rank_sum_unit_price_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "moved_at_month",
        "first_unit_price",
        "unit_price_dense_rank",
        "moved_at_year_avg_qty",
        "distinct_sku_id_in_operator",
        "unit_price_rank",
        "first_unit_price_max_3_prior",
        "unit_price_dense_rank_sum_unit_price_rank",
    )
)
