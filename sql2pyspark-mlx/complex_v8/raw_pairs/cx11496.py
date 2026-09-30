from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_distance_warehouse_id_by = Window.partitionBy("cnt_distance", "warehouse_id").orderBy(F.col("min_qty").desc_nulls_first(), "operator", "warehouse_id").rowsBetween(-1, Window.currentRow)
w_cnt_distance_warehouse_id_by_2 = Window.partitionBy("cnt_distance", "warehouse_id").orderBy(F.col("min_qty").desc_nulls_first(), "operator", "warehouse_id")
w_operator = Window.partitionBy("operator")

cleaned_moves = (
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
    )
)

grouped = (
    cleaned_moves
    .groupBy("operator", "warehouse_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("move_type").isin("INBOUND", "TRANSFER"), 1)) / F.count("*"), 1).alias("pct_move_type_not_in_inbound_tra"),
        F.count("*").alias("cnt"),
        F.sum("unit_price").alias("total_unit_price"),
        F.min("qty").alias("min_qty"),
    )
)

enriched = (
    grouped
    .select(
        "operator",
        "warehouse_id",
        "cnt",
        "total_unit_price",
        "min_qty",
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
        F.round(F.col("total_unit_price") * 1.1, 2).alias("total_unit_price_adj"),
    )
)

with_trends = (
    enriched
    .select(
        "operator",
        "warehouse_id",
        "cnt",
        "total_unit_price",
        "min_qty",
        "cnt_distance",
        "total_unit_price_adj",
        F.sum("cnt").over(w_cnt_distance_warehouse_id_by).alias("cnt_sum_2"),
        F.lead("cnt").over(w_cnt_distance_warehouse_id_by_2).alias("next_cnt"),
        F.lag("total_unit_price_adj").over(w_cnt_distance_warehouse_id_by_2).alias("prev_total_unit_price_adj"),
        (F.col("min_qty") - F.lag("min_qty").over(w_cnt_distance_warehouse_id_by_2)).alias("min_qty_change"),
        F.sum("total_unit_price_adj").over(w_operator).alias("operator_sum_total_unit_price_adj"),
    )
)

result = (
    with_trends
    .select(
        "operator",
        "warehouse_id",
        "total_unit_price",
        "min_qty",
        "cnt_distance",
        "cnt_sum_2",
        "next_cnt",
        "prev_total_unit_price_adj",
    )
    .orderBy("min_qty")
)
