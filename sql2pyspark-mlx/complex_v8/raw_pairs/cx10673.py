from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_unit_price_a = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc(), "id")
w_warehouse_id_by_unit_price_a_2 = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc(), "id").rowsBetween(-2, -1)
w_warehouse_id_by_unit_price_a_3 = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc(), "id").rowsBetween(-6, Window.currentRow)
w_move_type_by_unit_price_adj = Window.partitionBy("move_type").orderBy(F.col("unit_price_adj").asc_nulls_last())
w_warehouse_id_by_unit_price_a_4 = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_moved_at_month_sku_id_by_uni = Window.partitionBy(F.date_trunc("month", "moved_at"), "sku_id").orderBy("unit_price_distance", "id")
w_unit_price_adj_seq_unit_pric = Window.partitionBy("unit_price_adj_seq", "unit_price_distance_decile").orderBy(F.col("unit_price_distance_vs_warehouse_id_avg").desc(), "id")
w_all_by_qty_prior_1 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-1, -1)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_unit_price_adj_seq_unit_pric_2 = Window.partitionBy("unit_price_adj_seq", "unit_price_distance_decile").orderBy(F.col("unit_price_distance_vs_warehouse_id_avg").desc(), "id").rowsBetween(-1, -1)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.abs(F.col("unit_price") - 206.8).alias("unit_price_distance"),
    )
)

ranked = (
    recent_moves
    .select(
        "*",
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_by_unit_price_a)).alias("unit_price_change"),
        F.max("unit_price").over(w_warehouse_id_by_unit_price_a_2).alias("unit_price_max_2_prior"),
        F.row_number().over(w_warehouse_id_by_unit_price_a).alias("unit_price_adj_seq"),
        F.round(F.avg("unit_price").over(w_warehouse_id_by_unit_price_a_3), 2).alias("unit_price_avg_7"),
        F.round(F.cume_dist().over(w_move_type_by_unit_price_adj), 3).alias("unit_price_adj_cume_dist"),
        F.round(F.cume_dist().over(w_warehouse_id_by_unit_price_a_4), 3).alias("unit_price_adj_cume_dist_2"),
        F.round(F.col("unit_price_distance") - F.avg("unit_price_distance").over(w_warehouse_id), 2).alias("unit_price_distance_vs_warehouse_id_avg"),
        F.ntile(10).over(w_moved_at_month_sku_id_by_uni).alias("unit_price_distance_decile"),
        F.max("unit_price_distance").over(w_warehouse_id).alias("warehouse_id_max_unit_price_distance"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price_adj",
        "unit_price_adj_seq",
        "unit_price_distance_vs_warehouse_id_avg",
        "warehouse_id_max_unit_price_distance",
        "warehouse_id_max_unit_price",
        F.round(F.percent_rank().over(w_unit_price_adj_seq_unit_pric), 3).alias("unit_price_distance_vs_warehouse_id_avg_percent_"),
        F.sum("unit_price_adj_cume_dist").over(w_all_by_qty_prior_1).alias("unit_price_adj_cume_dist_sum_1_prior"),
        F.round(F.cume_dist().over(w_unit_price_adj_seq_unit_pric), 3).alias("unit_price_distance_vs_warehouse_id_avg_cume_dis"),
        F.ntile(4).over(w_all_by_qty).alias("qty_quartile"),
        F.sum("qty").over(w_unit_price_adj_seq_unit_pric_2).alias("qty_sum_1_prior"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "moved_at",
        "unit_price_adj_seq",
        "unit_price_distance_vs_warehouse_id_avg",
        "warehouse_id_max_unit_price_distance",
        "warehouse_id_max_unit_price",
        "unit_price_distance_vs_warehouse_id_avg_percent_",
        "unit_price_distance_vs_warehouse_id_avg_cume_dis",
        "qty_sum_1_prior",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
    .orderBy(
        F.col("unit_price_adj_seq").desc_nulls_first(),
        F.col("warehouse_id_max_unit_price_distance").desc(),
        "id",
    )
    .limit(5)
)
