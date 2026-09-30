from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_move_qty_moving_1 = Window.orderBy("max_move_qty", "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_max_move_qty = Window.orderBy("max_move_qty", "id")

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") > 189.4) & (F.col("warehouse_id").isNotNull()))
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

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("qty").alias("max_move_qty"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_unit_price",
        "st.max_move_qty",
        "st.first_moved_at",
    )
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.max("avg_move_unit_price").over(w_all_by_max_move_qty_moving_1).alias("avg_move_unit_price_max_2"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.rank().over(w_all_by_max_move_qty).alias("max_move_qty_rank"),
        F.lag("moved_at").over(w_all_by_max_move_qty).alias("prev_moved_at"),
        F.round(F.col("avg_move_unit_price") - F.avg("avg_move_unit_price").over(w_warehouse_id_sku_id), 2).alias("avg_move_unit_price_vs_warehouse_id_sku_id_avg"),
    )
)

with_details_warehouse = (
    metrics.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city") == "Singapore")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.avg_move_unit_price",
        "s.max_move_qty",
        "s.avg_move_unit_price_max_2",
        "s.rows_in_warehouse_id_sku_id",
        "s.avg_move_unit_price_vs_warehouse_id_sku_id_avg",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_details_warehouse
    .groupBy("sku_id", "operator")
    .pivot("warehouse_city", ["Singapore", "Bangkok"])
    .agg(F.sum("avg_move_unit_price_vs_warehouse_id_sku_id_avg").alias("total"), F.max("avg_move_unit_price_vs_warehouse_id_sku_id_avg").alias("max_v"))
)
