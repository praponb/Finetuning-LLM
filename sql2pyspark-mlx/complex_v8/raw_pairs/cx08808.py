from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_moved_at_year_b = Window.partitionBy("warehouse_id", F.year("moved_at")).orderBy(F.col("qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_moved_at_year_b_2 = Window.partitionBy("warehouse_id", F.year("moved_at")).orderBy(F.col("qty").desc(), "id").rowsBetween(-2, -1)
w_warehouse_id_moved_at_year_b_3 = Window.partitionBy("warehouse_id", F.year("moved_at")).orderBy(F.col("qty").desc(), "id").rowsBetween(-3, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

filtered = (
    recent
    .crossJoin(recent.agg(F.min("unit_price").alias("overall_min_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_min_unit_price"))
    .drop("overall_min_unit_price")
)

with_windows = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        F.row_number().over(w_warehouse_id_moved_at_year_b).alias("rn"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.round(F.avg("qty").over(w_warehouse_id_moved_at_year_b_2), 2).alias("qty_avg_2_prior"),
        F.max("qty").over(w_warehouse_id_moved_at_year_b_3).alias("qty_max_4"),
    )
)

joined_warehouse = (
    with_windows.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city").isin("Bangkok", "Kuala Lumpur")), "inner")
    .filter(F.col("w.capacity") <= 11005)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.rn",
        "s.rows_in_warehouse_id",
        "s.qty_avg_2_prior",
        "s.qty_max_4",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    joined_warehouse
    .groupBy("warehouse_id", "rn")
    .pivot("warehouse_city", ["Singapore", "Bangkok"])
    .agg(F.min("qty_avg_2_prior"))
)
