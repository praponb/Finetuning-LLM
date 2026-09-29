from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_filled_ful = Window.orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_filled = Window.orderBy("unit_price_filled", "id")
w_sku_id_warehouse_id_by_unit = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_unit_price_filled_run = Window.orderBy(F.col("unit_price_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_filled_2 = Window.orderBy(F.col("unit_price_filled").desc(), "id")
w_sku_id_warehouse_id_by_unit_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("unit_price").desc())
w_sku_id_warehouse_id_by_unit_3 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price_filled").desc_nulls_first(), "id")

filtered_moves = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(8, 10)) & (F.col("move_type").isin("INBOUND", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

ranked = (
    filtered_moves
    .select(
        "*",
        F.nth_value("unit_price_filled", 2).over(w_all_by_unit_price_filled_ful).alias("second_unit_price_filled"),
        F.dense_rank().over(w_all_by_unit_price_filled).alias("unit_price_filled_dense_rank"),
        F.first("moved_at", ignorenulls=True).over(w_sku_id_warehouse_id_by_unit).alias("first_known_moved_at"),
        F.min("qty").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_qty"),
        F.round(F.avg("qty").over(w_sku_id_warehouse_id), 2).alias("sku_id_warehouse_id_avg_qty"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_unit_price_filled_run).alias("last_known_moved_at"),
        F.lead("qty", 2).over(w_all_by_unit_price_filled_2).alias("next2_qty"),
        F.rank().over(w_sku_id_warehouse_id_by_unit_2).alias("unit_price_rank"),
        F.row_number().over(w_sku_id_warehouse_id_by_unit_3).alias("rn"),
        F.round(F.col("unit_price_filled") / F.nullif(F.sum("unit_price_filled").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("unit_price_filled_share_of_sku_id_warehouse_id"),
        F.lead("moved_at", 3).over(w_sku_id_operator_by_unit_pric).alias("next3_moved_at"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_unit_3), 3).alias("unit_price_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_filled",
        "move_type_or_unknown",
        "first_known_moved_at",
        "sku_id_warehouse_id_min_qty",
        "last_known_moved_at",
        "unit_price_rank",
        "unit_price_filled_share_of_sku_id_warehouse_id",
        "next3_moved_at",
        "unit_price_cume_dist",
    )
    .orderBy(F.col("next3_moved_at").desc_nulls_first(), "id")
    .limit(10)
)
