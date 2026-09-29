from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_move_type_or_unkno = Window.partitionBy("move_type", "move_type_or_unknown")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_move_type_or_unknown_by_move = Window.partitionBy("move_type_or_unknown").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_moving_1 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, Window.currentRow)
w_move_type_or_unknown_move_ty = Window.partitionBy("move_type_or_unknown", "move_type").orderBy("unit_price")
w_sku_id_by_unit_price_ties = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc_nulls_first())
w_move_type_or_unknown_by_move_2 = Window.partitionBy("move_type_or_unknown").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_or_unknown_by_unit = Window.partitionBy("operator_or_unknown").orderBy(F.col("unit_price").desc(), "id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_move_type_or_unknown_move_ty_2 = Window.partitionBy("move_type_or_unknown", "move_type").orderBy("unit_price", "id")
w_move_type_or_unknown_by_unit = Window.partitionBy("move_type_or_unknown").orderBy("unit_price_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_unit_price_c = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_cume_dist_2").asc_nulls_last(), "id")

valid_moves = (
    df_stock_moves
    .filter((F.col("qty").between(99, 288)) & (F.col("move_type") != "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

windowed = (
    valid_moves
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_move_type_or_unkno), 2).alias("unit_price_vs_move_type_move_type_or_unknown_avg"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
        F.lag("moved_at", 2).over(w_move_type_or_unknown_by_move).alias("prev2_moved_at"),
        F.first("moved_at").over(w_sku_id_by_unit_price_full).alias("first_moved_at"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_1), 2).alias("unit_price_avg_2"),
        F.round(F.cume_dist().over(w_move_type_or_unknown_move_ty), 3).alias("unit_price_cume_dist"),
        F.size(F.collect_set("operator_or_unknown").over(w_sku_id_warehouse_id)).alias("distinct_operator_or_unknown_in_sku_id_warehouse"),
        F.rank().over(w_sku_id_by_unit_price_ties).alias("unit_price_rank"),
        F.first("moved_at").over(w_move_type_or_unknown_by_move_2).alias("first_moved_at_2"),
        F.lead("unit_price").over(w_operator_or_unknown_by_unit).alias("next_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_warehouse_id), 2).alias("unit_price_vs_sku_id_warehouse_id_avg"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist_2"),
        F.lead("unit_price", 2, 0).over(w_move_type_or_unknown_move_ty_2).alias("next2_unit_price"),
    )
)

leaders = windowed.filter(F.col("unit_price_rank") == 1)

with_trends = (
    leaders
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "move_type_or_unknown",
        "unit_price_vs_move_type_move_type_or_unknown_avg",
        "rows_in_sku_id_warehouse_id",
        "unit_price_quintile",
        "unit_price_avg_2",
        "unit_price_cume_dist",
        "distinct_operator_or_unknown_in_sku_id_warehouse",
        "first_moved_at_2",
        "next_unit_price",
        "next2_unit_price",
        F.nth_value("unit_price_quintile", 2).over(w_move_type_or_unknown_by_unit).alias("second_unit_price_quintile"),
        F.round(F.percent_rank().over(w_warehouse_id_by_unit_price_c), 3).alias("unit_price_cume_dist_2_percent_rank"),
        F.last("unit_price").over(w_move_type_or_unknown_by_unit).alias("last_unit_price"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "move_type_or_unknown",
        "unit_price_vs_move_type_move_type_or_unknown_avg",
        "rows_in_sku_id_warehouse_id",
        "unit_price_quintile",
        "unit_price_avg_2",
        "unit_price_cume_dist",
        "next_unit_price",
        "next2_unit_price",
        "second_unit_price_quintile",
        "last_unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
    .orderBy("moved_at", F.col("move_type").desc())
)
