from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_days_since_moved_at = Window.orderBy("days_since_moved_at", "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_moved_at_to_date_by = Window.partitionBy("operator", F.to_date("moved_at")).orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_operator_moved_at_to_date_by_2 = Window.partitionBy("operator", F.to_date("moved_at")).orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_moved_at_r = Window.orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.ntile(3).over(w_move_type_by_moved_at).alias("moved_at_tercile"),
        (F.col("unit_price") - F.lag("unit_price").over(w_move_type_by_moved_at)).alias("unit_price_change"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_days_since_moved_at)).alias("unit_price_change_2"),
        F.sum("unit_price").over(w_sku_id).alias("sku_id_sum_unit_price"),
        F.sum("qty").over(w_operator_warehouse_id).alias("operator_warehouse_id_sum_qty"),
        F.row_number().over(w_all_by_qty).alias("rn"),
        F.nth_value("days_since_moved_at", 2).over(w_sku_id_move_type_by_moved_at).alias("second_days_since_moved_at"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.round(F.cume_dist().over(w_operator_moved_at_to_date_by), 3).alias("moved_at_cume_dist"),
        F.max("qty").over(w_operator_moved_at_to_date_by_2).alias("qty_max_2"),
        F.sum("qty").over(w_all_by_days_since_moved_at_r).alias("running_qty"),
    )
)

enriched = (
    sequenced
    .withColumn("rows_in_sku_id_per_unit_price", F.round(F.col("rows_in_sku_id") / F.nullif(F.col("unit_price"), F.lit(0)), 3))
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "move_type_slug",
        "moved_at_tercile",
        "unit_price_change",
        "rows_in_sku_id",
        "unit_price_change_2",
        "sku_id_sum_unit_price",
        "operator_warehouse_id_sum_qty",
        "second_days_since_moved_at",
        "unit_price_vs_sku_id_avg",
        "moved_at_cume_dist",
        "qty_max_2",
        "running_qty",
        F.round(F.col("moved_at_cume_dist") / F.nullif(F.col("unit_price_vs_sku_id_avg"), F.lit(0)), 3).alias("moved_at_cume_dist_per_unit_price_vs_sku_id_avg"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)
