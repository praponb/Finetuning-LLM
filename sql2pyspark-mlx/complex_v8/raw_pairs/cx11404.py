from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_warehouse_c = Window.partitionBy(F.to_date("moved_at"), "warehouse_city").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_moved_at_to_date_warehouse_c_2 = Window.partitionBy(F.to_date("moved_at"), "warehouse_city").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_to_date_warehouse_c_3 = Window.partitionBy(F.to_date("moved_at"), "warehouse_city").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, -1)
w_sku_id_warehouse_city_by_mov = Window.partitionBy("sku_id", "warehouse_city").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_opened_on_year_by = Window.partitionBy(F.year("warehouse_opened_on")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_city_by_mov_2 = Window.partitionBy("sku_id", "warehouse_city").orderBy("moved_at", "id")
w_distinct_operator_in_warehou = Window.partitionBy("distinct_operator_in_warehouse_city").orderBy(F.col("moved_at").desc())
w_second_warehouse_opened_on_m = Window.partitionBy(F.month("second_warehouse_opened_on"), "sku_id")

filtered = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("moved_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

combined_details_warehouse = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_windows = (
    combined_details_warehouse
    .select(
        "*",
        F.nth_value("warehouse_opened_on", 2).over(w_moved_at_to_date_warehouse_c).alias("second_warehouse_opened_on"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_city), F.lit(0)), 4).alias("unit_price_share_of_warehouse_city"),
        F.count(F.lit(1)).over(w_warehouse_city_warehouse_id).alias("rows_in_warehouse_city_warehouse_id"),
        F.sum("unit_price").over(w_moved_at_to_date_warehouse_c_2).alias("running_unit_price"),
        F.dense_rank().over(w_moved_at_to_date_warehouse_c_3).alias("warehouse_opened_on_dense_rank"),
        F.sum("unit_price").over(w_operator_sku_id_by_moved_at).alias("unit_price_sum_6_prior"),
        F.max("unit_price").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_max_unit_price"),
        F.size(F.collect_set("operator").over(w_warehouse_city)).alias("distinct_operator_in_warehouse_city"),
        F.min("unit_price").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_min_unit_price"),
        F.max("unit_price").over(w_sku_id_warehouse_city_by_mov).alias("max_unit_price_so_far"),
        F.first("moved_at").over(w_moved_at_to_date_warehouse_c).alias("first_moved_at"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_warehouse_opened_on_year_by).alias("last_known_warehouse_opened_on"),
        F.lead("warehouse_opened_on").over(w_sku_id_warehouse_city_by_mov_2).alias("next_warehouse_opened_on"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "warehouse_opened_on",
        "warehouse_city",
        "sku_labels",
        "unit_price_share_of_warehouse_city",
        "rows_in_warehouse_city_warehouse_id",
        "running_unit_price",
        "warehouse_opened_on_dense_rank",
        "unit_price_sum_6_prior",
        "warehouse_city_warehouse_id_max_unit_price",
        "distinct_operator_in_warehouse_city",
        "warehouse_city_warehouse_id_min_unit_price",
        "first_moved_at",
        "last_known_warehouse_opened_on",
        "next_warehouse_opened_on",
        F.round(F.cume_dist().over(w_distinct_operator_in_warehou), 3).alias("moved_at_cume_dist"),
        F.min("unit_price").over(w_second_warehouse_opened_on_m).alias("second_warehouse_opened_on_month_sku_id_min_unit"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_second_warehouse_opened_on_m), F.lit(0)), 4).alias("unit_price_share_of_second_warehouse_opened_on_m"),
    )
)

result = (
    windowed
    .filter(F.col("running_unit_price") < 3000.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "warehouse_city",
        "sku_labels",
        "unit_price_share_of_warehouse_city",
        "rows_in_warehouse_city_warehouse_id",
        "running_unit_price",
        "unit_price_sum_6_prior",
        "distinct_operator_in_warehouse_city",
        "warehouse_city_warehouse_id_min_unit_price",
        "last_known_warehouse_opened_on",
        "moved_at_cume_dist",
        "second_warehouse_opened_on_month_sku_id_min_unit",
        F.least(F.col("running_unit_price"), F.col("unit_price_sum_6_prior")).alias("least_running_unit_price_unit_price_sum_6_prior"),
    )
    .orderBy(
        F.col("distinct_operator_in_warehouse_city").asc_nulls_last(),
        F.col("last_known_warehouse_opened_on").asc(),
        "id",
    )
    .limit(10)
)
