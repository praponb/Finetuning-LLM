from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.date_trunc("month", "moved_at"))
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at")

base = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

sequenced = (
    base
    .select(
        "*",
        F.sum("unit_price").over(w_warehouse_id_moved_at_month).alias("warehouse_id_moved_at_month_sum_unit_price"),
        F.round(F.avg("unit_price").over(w_warehouse_id_moved_at_month), 2).alias("warehouse_id_moved_at_month_avg_unit_price"),
        F.rank().over(w_sku_id_move_type_by_moved_at).alias("moved_at_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_moved_at_month), 2).alias("qty_vs_warehouse_id_moved_at_month_avg"),
    )
)

combined_details_warehouse = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("K%"))
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code"))
)

result = (
    combined_details_warehouse
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "warehouse_id_moved_at_month_sum_unit_price",
        "warehouse_id_moved_at_month_avg_unit_price",
        "qty_vs_warehouse_id_moved_at_month_avg",
        "warehouse_wh_code",
    )
    .orderBy(F.col("warehouse_wh_code").desc(), F.col("id").asc_nulls_last())
)
