from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_all_by_qty = Window.orderBy(F.col("qty").desc_nulls_first(), "id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_warehouse_id_by_qty_moving_3 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-3, Window.currentRow)
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_warehouse_id_by_qty_prior_3 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-3, -1)
w_distinct_warehouse_id_in_mov = Window.partitionBy("distinct_warehouse_id_in_move_type_sku_id").orderBy(F.col("qty_cume_dist").desc(), "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_rn_move_type_by_max_qty_so_f = Window.partitionBy("rn", "move_type").orderBy(F.col("max_qty_so_far").desc(), "id").rowsBetween(-3, Window.currentRow)
w_rn_move_type_by_max_qty_so_f_2 = Window.partitionBy("rn", "move_type").orderBy(F.col("max_qty_so_far").desc(), "id")
w_move_type_by_max_qty_so_far = Window.partitionBy("move_type").orderBy(F.col("max_qty_so_far").desc_nulls_first())
w_move_type_by_max_qty_so_far_2 = Window.partitionBy("move_type").orderBy(F.col("max_qty_so_far").desc_nulls_first(), "id")
w_all_by_n_rows = Window.orderBy(F.col("n_rows").desc(), "sku_id", "sku_id_avg_qty_tier", "warehouse_id")

prepared = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
    )
)

with_parent_warehouse = (
    prepared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code"))
)

ranked = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        F.nth_value("qty", 2).over(w_warehouse_id_by_qty_full).alias("second_qty"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_sku_id)).alias("distinct_warehouse_id_in_move_type_sku_id"),
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.lag("moved_at", 2).over(w_warehouse_id_by_qty).alias("prev2_moved_at"),
        F.round(F.avg("qty").over(w_warehouse_id_by_qty_moving_3), 2).alias("qty_avg_4"),
        F.max("qty").over(w_all_by_qty_running).alias("max_qty_so_far"),
        F.max("qty").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_qty"),
        F.max("qty").over(w_warehouse_id_by_qty_prior_3).alias("qty_max_3_prior"),
        F.lead("moved_at", 3).over(w_all_by_qty).alias("next3_moved_at"),
        F.row_number().over(w_warehouse_id_by_qty).alias("rn"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "sku_id_avg_qty",
        "distinct_warehouse_id_in_move_type_sku_id",
        "qty_cume_dist",
        "prev2_moved_at",
        "qty_avg_4",
        "max_qty_so_far",
        "move_type_warehouse_id_max_qty",
        "next3_moved_at",
        F.ntile(3).over(w_distinct_warehouse_id_in_mov).alias("qty_cume_dist_tercile"),
        F.min("second_qty").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_second_qty"),
        F.round(F.avg("sku_id_avg_qty").over(w_rn_move_type_by_max_qty_so_f), 2).alias("sku_id_avg_qty_avg_4"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_sku_id)).alias("distinct_move_type_in_warehouse_id_sku_id"),
        F.round(F.percent_rank().over(w_rn_move_type_by_max_qty_so_f_2), 3).alias("max_qty_so_far_percent_rank"),
        F.dense_rank().over(w_move_type_by_max_qty_so_far).alias("max_qty_so_far_dense_rank"),
        F.lag("qty", 3).over(w_move_type_by_max_qty_so_far_2).alias("prev3_qty"),
        F.min("qty_cume_dist").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_qty_cume_dist"),
    )
)

totals = (
    ranked_2
    .groupBy(
        "sku_id",
        F.when(F.col("sku_id_avg_qty") >= 325.4, "A").when(F.col("sku_id_avg_qty") >= 200.6, "B").otherwise("C").alias("sku_id_avg_qty_tier"),
        "warehouse_id",
    )
    .agg(
        F.max("next3_moved_at").alias("last_next3_moved_at"),
        F.max("qty_avg_4").alias("max_qty_avg_4"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

ranked_3 = (
    totals
    .select(
        "*",
        F.min("n_rows").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_n_rows"),
        F.lag("last_next3_moved_at", 3).over(w_all_by_n_rows).alias("prev3_last_next3_moved_at"),
    )
)

result = (
    ranked_3
    .filter(F.to_date(F.col("prev3_last_next3_moved_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "sku_id",
        "warehouse_id",
        "max_qty_avg_4",
        "warehouse_id_sku_id_min_n_rows",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.when(F.col("n_rows") >= 9, "A").when(F.col("n_rows") >= 5, "B").otherwise("C").alias("n_rows_tier"),
    )
    .orderBy(F.col("n_rows_tier").asc())
)
