from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_qty = Window.orderBy("qty", "id")

prepared = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") < 372.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

latest_per_key = (
    prepared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_warehouse = (
    latest_per_key.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_clean",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

windowed = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "operator_clean",
        F.sum("qty").over(w_all_by_moved_at_running).alias("running_qty"),
        F.round(F.percent_rank().over(w_all_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.min("qty").over(w_sku_id).alias("sku_id_min_qty"),
        F.ntile(5).over(w_all_by_qty).alias("qty_quintile"),
    )
)

result = (
    windowed
    .groupingSets(
        [["operator", "sku_id", "warehouse_id"], ["operator"], []],
        "operator", "sku_id", "warehouse_id",
    )
    .agg(F.min("moved_at").alias("first_moved_at"))
    .orderBy(
        F.col("operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
