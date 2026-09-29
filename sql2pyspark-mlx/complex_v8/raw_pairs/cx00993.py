from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last())
w_moved_at_year = Window.partitionBy(F.year("moved_at"))
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_prior_1 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, -1)
w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy("moved_at")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").asc_nulls_last(), "id")
w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_move_type = Window.partitionBy("move_type")
w_moved_at_tercile = Window.partitionBy("moved_at_tercile")
w_sku_id_moved_at_tercile_by_f = Window.partitionBy("sku_id", "moved_at_tercile").orderBy("first_moved_at", "id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (~F.col("move_type").isin("ADJUST", "OUTBOUND")))
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

windowed = (
    filtered_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.round(F.cume_dist().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_cume_dist"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_year), 2).alias("unit_price_vs_moved_at_year_avg"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.max("unit_price").over(w_all_by_unit_price_prior_1).alias("unit_price_max_1_prior"),
        F.round(F.avg("qty").over(w_warehouse_id), 2).alias("warehouse_id_avg_qty"),
        F.round(F.cume_dist().over(w_sku_id_by_moved_at_ties), 3).alias("moved_at_cume_dist_2"),
        F.lead("qty", 2, 0).over(w_sku_id_by_moved_at).alias("next2_qty"),
        F.ntile(4).over(w_move_type_by_qty).alias("qty_quartile"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_by_moved_at_ful).alias("second_moved_at"),
        F.first("moved_at").over(w_warehouse_id_by_moved_at_ful).alias("first_moved_at"),
        F.dense_rank().over(w_warehouse_id_by_moved_at_tie).alias("moved_at_dense_rank"),
        F.ntile(3).over(w_warehouse_id_by_moved_at).alias("moved_at_tercile"),
        F.sum("unit_price").over(w_moved_at_year).alias("moved_at_year_sum_unit_price"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.count(F.lit(1)).over(w_moved_at_tercile).alias("rows_in_moved_at_tercile"),
        F.round(F.cume_dist().over(w_sku_id_moved_at_tercile_by_f), 3).alias("first_moved_at_cume_dist"),
        F.round(F.col("moved_at_year_sum_unit_price") - F.avg("moved_at_year_sum_unit_price").over(w_moved_at_tercile), 2).alias("moved_at_year_sum_unit_price_vs_moved_at_tercile"),
        F.row_number().over(w_sku_id_moved_at_tercile_by_f).alias("rn"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "moved_at_cume_dist",
        "unit_price_vs_moved_at_year_avg",
        "unit_price_rank",
        "qty_vs_warehouse_id_avg",
        "unit_price_max_1_prior",
        "moved_at_cume_dist_2",
        "next2_qty",
        "qty_quartile",
        "second_moved_at",
        "moved_at_tercile",
        "moved_at_year_sum_unit_price",
        "distinct_sku_id_in_move_type",
        "first_moved_at_cume_dist",
        "moved_at_year_sum_unit_price_vs_moved_at_tercile",
        "rn",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
    .orderBy(F.col("qty_quartile").desc_nulls_first(), "rn")
)
