from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy("qty")
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_move_type_by_moved_at_2 = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id")
w_move_type_operator = Window.partitionBy("move_type", "operator")

filtered_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
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

metrics = (
    filtered_moves
    .select(
        "*",
        F.lag("qty").over(w_sku_id_by_qty).alias("prev_qty"),
        F.ntile(10).over(w_all_by_moved_at).alias("moved_at_decile"),
        F.rank().over(w_all_by_unit_price).alias("unit_price_rank"),
        F.rank().over(w_operator_by_qty_ties).alias("qty_rank"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.nth_value("unit_price", 2).over(w_sku_id_move_type_by_moved_at).alias("second_unit_price"),
        F.round(F.percent_rank().over(w_operator_by_qty_ties), 3).alias("qty_percent_rank"),
        F.lag("qty", 2, 0).over(w_sku_id_by_qty).alias("prev2_qty"),
        F.nth_value("unit_price", 2).over(w_all_by_moved_at_full).alias("second_unit_price_2"),
        F.round(F.cume_dist().over(w_all_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
        F.ntile(5).over(w_sku_id_move_type_by_moved_at_2).alias("moved_at_quintile"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_share_of_move_type_operator"),
    )
)

first_rows = metrics.filter(F.col("qty_rank") == 1)

result = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "prev_qty",
        "unit_price_rank",
        "qty_rank",
        "sku_id_min_unit_price",
        "second_unit_price",
        "qty_percent_rank",
        "prev2_qty",
        "second_unit_price_2",
        "moved_at_cume_dist",
        "qty_share_of_sku_id",
        "moved_at_quintile",
        "distinct_operator_in_sku_id",
        "unit_price_share_of_move_type_operator",
        F.when(F.col("prev_qty") >= 392, "A").when(F.col("prev_qty") >= 154, "B").otherwise("C").alias("prev_qty_tier"),
    )
    .orderBy("prev2_qty")
)
