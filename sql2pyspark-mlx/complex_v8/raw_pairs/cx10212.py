from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type_by_qty_ties = Window.partitionBy("sku_id", "move_type").orderBy("qty")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_sku_id_move_type_by_qty = Window.partitionBy("sku_id", "move_type").orderBy("qty", "id")

recent = (
    df_stock_moves
    .filter(F.col("move_type") != "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

metrics = (
    with_activity
    .select(
        "id",
        "qty",
        "unit_price",
        "moved_at_label",
        F.round(F.percent_rank().over(w_sku_id_move_type_by_qty_ties), 3).alias("qty_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.round(F.avg("qty").over(w_warehouse_id_move_type), 2).alias("warehouse_id_move_type_avg_qty"),
        F.lead("unit_price").over(w_sku_id_move_type_by_qty).alias("next_unit_price"),
        F.rank().over(w_sku_id_move_type_by_qty_ties).alias("qty_rank"),
        F.rank().over(w_sku_id_move_type_by_qty).alias("qty_rank_2"),
    )
)

result = (
    metrics
    .cube("qty_rank", "qty_rank_2")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("qty_rank").alias("total_qty_rank"),
    )
    .orderBy(
        F.col("qty_rank").asc_nulls_last(),
        F.col("qty_rank_2").asc_nulls_last(),
    )
)
