from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy(F.col("unit_price").desc_nulls_first())
w_all_by_stddev_sku_id_operato = Window.orderBy("stddev_sku_id_operator_avg_qty_plus_unit_price", "warehouse_id")
w_distinct_warehouse_ids_wareh = Window.partitionBy("distinct_warehouse_ids", "warehouse_id").orderBy(F.col("distinct_warehouse_ids").desc(), "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_stddev_sku_id_operato_2 = Window.orderBy("stddev_sku_id_operator_avg_qty_plus_unit_price", "warehouse_id").rowsBetween(-3, Window.currentRow)
w_distinct_warehouse_ids_by_to = Window.partitionBy("distinct_warehouse_ids").orderBy("total_sku_id_operator_sum_qty_plus_unit_price", "warehouse_id")

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("unit_price") < 396.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "operator_slug",
        "qty_plus_unit_price",
        F.round(F.avg("qty_plus_unit_price").over(w_sku_id_operator), 2).alias("sku_id_operator_avg_qty_plus_unit_price"),
        (F.col("qty_plus_unit_price") - F.lag("qty_plus_unit_price").over(w_operator_by_unit_price)).alias("qty_plus_unit_price_change"),
        F.count(F.lit(1)).over(w_sku_id_by_unit_price_running).alias("running_count"),
        F.round(F.cume_dist().over(w_operator_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.sum("qty_plus_unit_price").over(w_sku_id_operator).alias("sku_id_operator_sum_qty_plus_unit_price"),
    )
)

per_group = (
    sequenced
    .filter(F.col("qty_plus_unit_price") >= 200.8)
    .groupBy("warehouse_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.sum("running_count") / F.nullif(F.sum("sku_id_operator_avg_qty_plus_unit_price"), F.lit(0)), 3).alias("running_count_to_sku_id_operator_avg_qty_plus_un"),
        F.round(F.stddev("sku_id_operator_avg_qty_plus_unit_price"), 2).alias("stddev_sku_id_operator_avg_qty_plus_unit_price"),
        F.round(F.sum("sku_id_operator_sum_qty_plus_unit_price"), 2).alias("total_sku_id_operator_sum_qty_plus_unit_price"),
        F.sum("qty_plus_unit_price_change").alias("sum_qty_plus_unit_price_change_check"),
    )
    .filter(F.col("sum_qty_plus_unit_price_change_check") > -500.0)
    .drop("sum_qty_plus_unit_price_change_check")
)

ranked = (
    per_group
    .select(
        "warehouse_id",
        "operator_list",
        "distinct_warehouse_ids",
        "running_count_to_sku_id_operator_avg_qty_plus_un",
        "stddev_sku_id_operator_avg_qty_plus_unit_price",
        "total_sku_id_operator_sum_qty_plus_unit_price",
        F.row_number().over(w_all_by_stddev_sku_id_operato).alias("stddev_sku_id_operator_avg_qty_plus_unit_price_s"),
        F.nth_value("distinct_warehouse_ids", 2).over(w_distinct_warehouse_ids_wareh).alias("second_distinct_warehouse_ids"),
        F.sum("distinct_warehouse_ids").over(w_all_by_stddev_sku_id_operato_2).alias("distinct_warehouse_ids_sum_4"),
        F.lag("distinct_warehouse_ids").over(w_distinct_warehouse_ids_by_to).alias("prev_distinct_warehouse_ids"),
    )
)

result = (
    ranked
    .select(
        "warehouse_id",
        "operator_list",
        "running_count_to_sku_id_operator_avg_qty_plus_un",
        "stddev_sku_id_operator_avg_qty_plus_unit_price_s",
        "second_distinct_warehouse_ids",
        "distinct_warehouse_ids_sum_4",
        "prev_distinct_warehouse_ids",
    )
)
