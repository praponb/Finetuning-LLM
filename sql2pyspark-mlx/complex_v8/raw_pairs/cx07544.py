from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_moving_1 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_all_by_unit_price_moving_3 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_distinct_sku_id = Window.partitionBy("warehouse_id", "distinct_sku_id_in_warehouse_id")
w_all_by_seq_no = Window.orderBy(F.col("seq_no").asc_nulls_last(), "id")
w_seq_no_distance = Window.partitionBy("seq_no_distance")

scoped = (
    df_stock_moves
    .filter(F.col("unit_price") <= 367.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_warehouse_id), 2).alias("warehouse_id_avg_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.round(F.avg("qty").over(w_all_by_unit_price_moving_1), 2).alias("qty_avg_2"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.round(F.avg("qty").over(w_all_by_unit_price_moving_3), 2).alias("qty_avg_4"),
        F.nth_value("qty", 2).over(w_all_by_unit_price_full).alias("second_qty"),
        F.last("qty", ignorenulls=True).over(w_all_by_unit_price_running).alias("last_known_qty"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
        F.row_number().over(w_all_by_unit_price).alias("seq_no"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty_avg_2",
        "unit_price_percent_rank",
        "distinct_sku_id_in_warehouse_id",
        "qty_avg_4",
        "last_known_qty",
        "seq_no",
        (F.col("unit_price") + F.col("qty_avg_4")).alias("unit_price_plus_qty_avg_4"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.abs(F.col("seq_no") - 4).alias("seq_no_distance"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "sku_id",
        "qty_avg_2",
        "distinct_sku_id_in_warehouse_id",
        "qty_avg_4",
        "last_known_qty",
        "seq_no",
        "unit_price_plus_qty_avg_4",
        "seq_no_distance",
        F.round(F.col("seq_no_distance") - F.avg("seq_no_distance").over(w_warehouse_id_distinct_sku_id), 2).alias("seq_no_distance_vs_warehouse_id_distinct_sku_id_"),
        F.row_number().over(w_all_by_seq_no).alias("seq_no_2"),
        F.min("unit_price_percent_rank").over(w_warehouse_id_distinct_sku_id).alias("warehouse_id_distinct_sku_id_in_warehouse_id_min"),
    )
)

per_group = (
    windowed
    .groupBy("seq_no_distance")
    .agg(
        F.round(F.stddev("seq_no"), 2).alias("stddev_seq_no"),
        F.round(F.sum("warehouse_id_distinct_sku_id_in_warehouse_id_min") / F.nullif(F.sum("seq_no_2"), F.lit(0)), 3).alias("warehouse_id_distinct_sku_id_in_warehouse_id_min_2"),
        F.round(F.stddev("distinct_sku_id_in_warehouse_id"), 2).alias("stddev_distinct_sku_id_in_warehouse_id"),
        F.sum("qty_avg_4").alias("sum_qty_avg_4_check"),
    )
    .filter(F.col("sum_qty_avg_4_check") > 175.7)
    .drop("sum_qty_avg_4_check")
)

metrics_2 = (
    per_group
    .select(
        "seq_no_distance",
        F.count(F.lit(1)).over(w_seq_no_distance).alias("rows_in_seq_no_distance"),
        F.min("stddev_seq_no").over(w_seq_no_distance).alias("seq_no_distance_min_stddev_seq_no"),
    )
)

result = (
    metrics_2
    .select(
        "seq_no_distance",
        "seq_no_distance_min_stddev_seq_no",
        F.round(F.col("rows_in_seq_no_distance") / F.nullif(F.col("seq_no_distance"), F.lit(0)), 3).alias("rows_in_seq_no_distance_per_seq_no_distance"),
        (F.col("seq_no_distance") - F.col("rows_in_seq_no_distance")).alias("seq_no_distance_minus_rows_in_seq_no_distance"),
    )
    .orderBy(F.col("seq_no_distance_minus_rows_in_seq_no_distance").asc())
)
