from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")
w_move_type = Window.partitionBy("move_type")
w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy("moved_at")
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_unit_price_f = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_moving_1 = Window.orderBy("unit_price", "id").rowsBetween(-1, Window.currentRow)
w_latest = Window.partitionBy("sku_id", "move_type").orderBy("running_unit_price", "id")

prepared_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 14)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

sequenced = (
    prepared_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "unit_price_per_qty",
        F.lead("unit_price_per_qty", 1, 0).over(w_warehouse_id_by_unit_price).alias("next_unit_price_per_qty"),
        F.round(F.col("unit_price_per_qty") - F.avg("unit_price_per_qty").over(w_move_type), 2).alias("unit_price_per_qty_vs_move_type_avg"),
        F.dense_rank().over(w_sku_id_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.sum("unit_price").over(w_warehouse_id_by_unit_price_r).alias("running_unit_price"),
        F.first("qty").over(w_warehouse_id_by_unit_price_f).alias("first_qty"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_1), 2).alias("unit_price_avg_2"),
    )
)

unique_rows = (
    sequenced
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .groupingSets(
        [["sku_id", "moved_at_dense_rank"], ["sku_id"], []],
        "sku_id", "moved_at_dense_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("unit_price_avg_2") / F.nullif(F.sum("moved_at_dense_rank"), F.lit(0)), 3).alias("unit_price_avg_2_to_moved_at_dense_rank_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("move_type").isNotNull(), F.col("next_unit_price_per_qty")).otherwise(0)).alias("next_unit_price_per_qty_move_type_is_not_null"),
    )
)
