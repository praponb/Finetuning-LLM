from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_moved_at_to_date_by_q = Window.partitionBy("sku_id", F.to_date("moved_at")).orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_moved_at_full = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_moved_at_to_date = Window.partitionBy("move_type", F.to_date("moved_at"))
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_move_type_by_qt = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(-1, -1)

valid_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

metrics = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        F.sum("qty").over(w_sku_id_moved_at_to_date_by_q).alias("running_qty"),
        F.first("qty").over(w_move_type_by_moved_at_full).alias("first_qty"),
        F.last("qty", ignorenulls=True).over(w_warehouse_id_by_moved_at_run).alias("last_known_qty"),
        F.sum("qty").over(w_move_type_moved_at_to_date).alias("move_type_moved_at_to_date_sum_qty"),
        F.min("unit_price").over(w_warehouse_id).alias("warehouse_id_min_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type_moved_at_to_date), 2).alias("qty_vs_move_type_moved_at_to_date_avg"),
        F.rank().over(w_warehouse_id_by_moved_at).alias("moved_at_rank"),
        F.count(F.lit(1)).over(w_move_type_moved_at_to_date).alias("rows_in_move_type_moved_at_to_date"),
        F.round(F.avg("unit_price").over(w_warehouse_id_move_type_by_qt), 2).alias("unit_price_avg_1_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "running_qty",
        "last_known_qty",
        "move_type_moved_at_to_date_sum_qty",
        "warehouse_id_min_unit_price",
        "qty_share_of_warehouse_id",
        "qty_vs_move_type_moved_at_to_date_avg",
        "moved_at_rank",
        "rows_in_move_type_moved_at_to_date",
        "unit_price_avg_1_prior",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("running_qty").cast("string")).alias("move_type_ref"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("warehouse_id").desc())
    .limit(10)
)
