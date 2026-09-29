from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_opened_o = Window.partitionBy("move_type", F.year("warehouse_opened_on")).orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy("qty")
w_all_by_warehouse_opened_on = Window.orderBy(F.col("warehouse_opened_on").desc(), "id")
w_sku_id_by_warehouse_capacity = Window.partitionBy("sku_id").orderBy("warehouse_capacity", "id").rowsBetween(-3, -1)
w_sku_is_active_sku_id = Window.partitionBy("sku_is_active", "sku_id")
w_move_type_warehouse_opened_o_2 = Window.partitionBy("move_type", F.year("warehouse_opened_on")).orderBy(F.col("warehouse_opened_on").desc(), "id")
w_move_type_by_warehouse_capac = Window.partitionBy("move_type").orderBy("warehouse_capacity", "id").rowsBetween(-1, Window.currentRow)

base_moves = (
    df_stock_moves
    .filter(F.col("qty") < 256)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

combined_details_warehouse = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

windowed = (
    combined_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "warehouse_capacity",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "sku_labels",
        F.min("warehouse_capacity").over(w_move_type_warehouse_opened_o).alias("min_warehouse_capacity_so_far"),
        F.dense_rank().over(w_warehouse_id_by_qty).alias("qty_dense_rank"),
        F.round(F.cume_dist().over(w_warehouse_id_by_qty_ties), 3).alias("qty_cume_dist"),
        F.rank().over(w_all_by_warehouse_opened_on).alias("warehouse_opened_on_rank"),
        F.round(F.avg("unit_price").over(w_sku_id_by_warehouse_capacity), 2).alias("unit_price_avg_3_prior"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_sku_is_active_sku_id), 2).alias("warehouse_capacity_vs_sku_is_active_sku_id_avg"),
        F.lead("unit_price", 3).over(w_move_type_warehouse_opened_o_2).alias("next3_unit_price"),
        F.round(F.avg("qty").over(w_move_type_by_warehouse_capac), 2).alias("qty_avg_2"),
    )
)

totals = (
    windowed
    .groupBy(
        F.year("warehouse_opened_on").alias("warehouse_opened_on_year"),
        "warehouse_opened_on_rank",
        "qty_dense_rank",
    )
    .agg(
        F.sum("warehouse_capacity").alias("total_warehouse_capacity"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.max("warehouse_opened_on").alias("last_warehouse_opened_on"),
        F.round(F.stddev("qty_cume_dist"), 2).alias("stddev_qty_cume_dist"),
        F.count(F.when(F.length(F.trim(F.col("warehouse_wh_code"))) >= 6, 1)).alias("n_length_trim_warehouse_wh_cod"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .filter(F.col("last_warehouse_opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "warehouse_opened_on_year",
        "warehouse_opened_on_rank",
        "qty_dense_rank",
        "total_warehouse_capacity",
        "distinct_warehouse_ids",
        "last_warehouse_opened_on",
        "stddev_qty_cume_dist",
        "n_rows",
    )
    .orderBy(
        F.col("warehouse_opened_on_year").asc_nulls_last(),
        F.col("last_warehouse_opened_on").desc_nulls_first(),
    )
)
