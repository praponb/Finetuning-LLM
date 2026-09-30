from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_move_type_len = Window.partitionBy("move_type", "move_type_len")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id")
w_move_type_by_unit_price_full = Window.partitionBy("move_type").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_id_by_sku_u = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("sku_unit_cost").desc())
w_all_by_unit_price_moving_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("qty") < 387)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

with_details_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_len",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    with_details_sku
    .select(
        "*",
        F.nth_value("qty", 2).over(w_all_by_unit_price_full).alias("second_qty"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_move_type_len), F.lit(0)), 4).alias("unit_price_share_of_move_type_move_type_len"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.round(F.avg("qty").over(w_move_type_move_type_len), 2).alias("move_type_move_type_len_avg_qty"),
        F.lead("moved_at").over(w_warehouse_id_by_qty).alias("next_moved_at"),
        F.nth_value("qty", 2).over(w_move_type_by_unit_price_full).alias("second_qty_2"),
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_sku_u), 3).alias("sku_unit_cost_cume_dist"),
        F.sum("qty").over(w_all_by_unit_price_moving_1).alias("qty_sum_2"),
        F.max("move_type_len").over(w_sku_id).alias("sku_id_max_move_type_len"),
        F.ntile(5).over(w_warehouse_id_by_qty).alias("qty_quintile"),
        F.min("move_type_len").over(w_sku_id).alias("sku_id_min_move_type_len"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_type_len",
        "sku_code",
        "sku_labels",
        "sku_unit_cost",
        "second_qty",
        "unit_price_share_of_move_type_move_type_len",
        "moved_at_percent_rank",
        "move_type_move_type_len_avg_qty",
        "next_moved_at",
        "second_qty_2",
        "sku_unit_cost_cume_dist",
        "qty_sum_2",
        "sku_id_max_move_type_len",
        "qty_quintile",
        F.concat(F.col("sku_code"), F.lit("#"), F.col("id").cast("string")).alias("sku_code_ref"),
        F.concat_ws(": ", F.col("move_type"), F.col("sku_code")).alias("move_type_sku_code_label"),
    )
)
