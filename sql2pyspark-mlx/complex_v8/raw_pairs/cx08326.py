from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("warehouse_id").orderBy("last_known_qty", "id")

valid = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")) & (F.col("sku_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

ranked = (
    valid
    .select(
        "*",
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_move_type), 2).alias("qty_vs_sku_id_move_type_avg"),
        F.last("qty", ignorenulls=True).over(w_warehouse_id_move_type_by_mo).alias("last_known_qty"),
        F.min("qty").over(w_sku_id_move_type).alias("sku_id_move_type_min_qty"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
    )
)

latest_per_key = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupingSets(
        [["warehouse_id", "sku_id"], ["warehouse_id"]],
        "warehouse_id", "sku_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("operator").alias("distinct_operators"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
