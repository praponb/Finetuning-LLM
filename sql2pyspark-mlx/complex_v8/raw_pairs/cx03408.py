from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id_by_moved_at = Window.partitionBy("move_type", "sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_qty = Window.orderBy("qty", "id")
w_move_type_sku_id_by_moved_at_2 = Window.partitionBy("move_type", "sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id")
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at")
w_latest = Window.partitionBy("warehouse_id").orderBy(F.col("next_qty").desc(), F.col("id").desc())
w_operator_by_qty_cume_dist = Window.partitionBy("operator").orderBy(F.col("qty_cume_dist").asc_nulls_last(), "id")
w_operator_by_qty_cume_dist_ru = Window.partitionBy("operator").orderBy(F.col("qty_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("move_type").orderBy(F.col("unit_price_change").desc(), F.col("id").desc())

cleaned_moves = df_stock_moves.filter(F.col("unit_price") >= 236.5)

sequenced = (
    cleaned_moves
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_move_type_sku_id_by_moved_at), 2).alias("unit_price_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_sku_id_by_moved_at_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.lead("qty").over(w_warehouse_id_move_type_by_mo).alias("next_qty"),
        F.round(F.percent_rank().over(w_warehouse_id_move_type_by_mo_2), 3).alias("moved_at_percent_rank"),
    )
)

latest = (
    sequenced
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    latest
    .select(
        "*",
        (F.col("unit_price") - F.lag("unit_price").over(w_operator_by_qty_cume_dist)).alias("unit_price_change"),
        F.last("minutes_since_prev_moved_at", ignorenulls=True).over(w_operator_by_qty_cume_dist_ru).alias("last_known_minutes_since_prev_moved_at"),
    )
)

latest_2 = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_2
    .groupBy("move_type")
    .pivot("operator", ["op_dew", "op_eve", "op_ann", "op_bee"])
    .agg(F.avg("minutes_since_prev_moved_at"))
    .orderBy("move_type")
)
