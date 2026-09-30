from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_dayofweek_by_moved = Window.partitionBy("moved_at_dayofweek").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_moving_3 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-3, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_moved_at_dayofweek_warehouse = Window.partitionBy("moved_at_dayofweek", "warehouse_id").orderBy("qty", "id")
w_moved_at_dayofweek_by_moved_2 = Window.partitionBy("moved_at_dayofweek").orderBy("moved_at", "id")
w_moved_at_dayofweek_by_moved_3 = Window.partitionBy("moved_at_dayofweek").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_first_known_moved_at_dayofwe = Window.partitionBy("first_known_moved_at_dayofweek", "moved_at_dense_rank")

filtered = (
    df_stock_moves
    .filter((F.col("unit_price") < 396.4) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "moved_at_dayofweek",
        F.last("unit_price", ignorenulls=True).over(w_moved_at_dayofweek_by_moved).alias("last_known_unit_price"),
        F.sum("qty").over(w_all_by_qty_moving_3).alias("qty_sum_4"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.lead("qty").over(w_moved_at_dayofweek_warehouse).alias("next_qty"),
        F.dense_rank().over(w_moved_at_dayofweek_by_moved_2).alias("moved_at_dense_rank"),
        F.sum("unit_price").over(w_moved_at_dayofweek_by_moved_3).alias("unit_price_sum_2"),
        F.dense_rank().over(w_all_by_qty_ties).alias("qty_dense_rank"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.cume_dist().over(w_moved_at_dayofweek_by_moved_2), 3).alias("moved_at_cume_dist"),
        F.first("moved_at_dayofweek", ignorenulls=True).over(w_moved_at_dayofweek_by_moved).alias("first_known_moved_at_dayofweek"),
        F.max("moved_at_dayofweek").over(w_sku_id).alias("sku_id_max_moved_at_dayofweek"),
    )
)

with_trends = (
    ranked
    .select(
        "id",
        "sku_id",
        "unit_price",
        "moved_at_dayofweek",
        "last_known_unit_price",
        "qty_sum_4",
        "moved_at_dense_rank",
        "rows_in_sku_id",
        "moved_at_cume_dist",
        "sku_id_max_moved_at_dayofweek",
        F.count(F.lit(1)).over(w_first_known_moved_at_dayofwe).alias("rows_in_first_known_moved_at_dayofweek_moved_at_"),
        F.round(F.col("qty_vs_sku_id_avg") / F.nullif(F.sum("qty_vs_sku_id_avg").over(w_first_known_moved_at_dayofwe), F.lit(0)), 4).alias("qty_vs_sku_id_avg_share_of_first_known_moved_at_"),
        F.round(F.col("unit_price_sum_2") / F.nullif(F.sum("unit_price_sum_2").over(w_first_known_moved_at_dayofwe), F.lit(0)), 4).alias("unit_price_sum_2_share_of_first_known_moved_at_d"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "moved_at_dayofweek",
        "last_known_unit_price",
        "moved_at_dense_rank",
        "rows_in_sku_id",
        "moved_at_cume_dist",
        "sku_id_max_moved_at_dayofweek",
        "rows_in_first_known_moved_at_dayofweek_moved_at_",
        "qty_vs_sku_id_avg_share_of_first_known_moved_at_",
        "unit_price_sum_2_share_of_first_known_moved_at_d",
        F.abs(F.col("last_known_unit_price") - 264.8).alias("last_known_unit_price_distance"),
        F.abs(F.col("rows_in_sku_id") - 12).alias("rows_in_sku_id_distance"),
    )
    .orderBy(F.col("rows_in_sku_id_distance").asc())
)
