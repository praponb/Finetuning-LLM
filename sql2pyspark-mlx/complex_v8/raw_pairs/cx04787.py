from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_moved_at_running = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_to_date_by_unit_pri = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_moved_at_to_date_by_unit_pri_2 = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at_day").desc(), "id")

prepared = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

enriched_warehouse = (
    prepared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_day",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

ranked = (
    enriched_warehouse
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.sum("unit_price").over(w_sku_id).alias("sku_id_sum_unit_price"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_sku_id_by_moved_at_running).alias("last_known_warehouse_opened_on"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.round(F.percent_rank().over(w_moved_at_to_date_by_unit_pri), 3).alias("unit_price_percent_rank"),
        F.nth_value("warehouse_opened_on", 2).over(w_moved_at_to_date_by_unit_pri_2).alias("second_warehouse_opened_on"),
        F.rank().over(w_operator_sku_id_by_moved_at).alias("moved_at_day_rank"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.round(F.avg("unit_price").over(w_sku_id), 2).alias("sku_id_avg_unit_price"),
    )
)

rollup_base = (
    ranked
    .groupBy("moved_at_day_rank")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("warehouse_city").alias("distinct_warehouse_citys"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.sum("unit_price_share_of_sku_id").alias("total_unit_price_share_of_sku_id"),
        F.percentile_approx("moved_at_day_rank", 0.5).alias("median_moved_at_day_rank"),
        F.sum("unit_price").alias("sum_unit_price_check"),
    )
    .filter(F.col("sum_unit_price_check") > 180.0)
    .drop("sum_unit_price_check")
)

result = (
    rollup_base
    .filter(F.col("total_unit_price_share_of_sku_id") > 0.9)
    .select(
        "moved_at_day_rank",
        "cnt",
        "distinct_warehouse_citys",
        "warehouse_city_list",
        "total_unit_price_share_of_sku_id",
        F.abs(F.col("distinct_warehouse_citys") - 6).alias("distinct_warehouse_citys_distance"),
    )
    .orderBy(F.col("distinct_warehouse_citys_distance").desc_nulls_first())
)
