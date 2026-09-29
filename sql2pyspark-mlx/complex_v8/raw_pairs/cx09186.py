from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id_by_warehouse = Window.partitionBy("operator", "sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_warehouse_opened_on_year = Window.partitionBy(F.year("warehouse_opened_on"))
w_operator_sku_id_by_warehouse_2 = Window.partitionBy("operator", "sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-3, -1)
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_month_by_warehouse = Window.partitionBy(F.month("moved_at")).orderBy("warehouse_capacity")
w_moved_at_month_by_warehouse_2 = Window.partitionBy(F.month("moved_at")).orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_by_warehouse_3 = Window.partitionBy(F.month("moved_at")).orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_first_known_warehouse_opened = Window.partitionBy(F.date_trunc("month", "first_known_warehouse_opened_on"), "sku_id")
w_sku_id_operator_by_second_mo = Window.partitionBy("sku_id", "operator").orderBy(F.col("second_moved_at").desc(), "id")

scoped = (
    df_stock_moves
    .filter(F.col("qty") >= 154)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.lead("warehouse_capacity").over(w_operator_sku_id_by_warehouse).alias("next_warehouse_capacity"),
        F.sum("warehouse_capacity").over(w_warehouse_opened_on_year).alias("warehouse_opened_on_year_sum_warehouse_capacity"),
        F.max("unit_price").over(w_warehouse_opened_on_year).alias("warehouse_opened_on_year_max_unit_price"),
        F.round(F.avg("unit_price").over(w_operator_sku_id_by_warehouse_2), 2).alias("unit_price_avg_3_prior"),
        F.nth_value("moved_at", 2).over(w_all_by_unit_price_full).alias("second_moved_at"),
        F.round(F.cume_dist().over(w_moved_at_month_by_warehouse), 3).alias("warehouse_capacity_cume_dist"),
        F.first("warehouse_opened_on", ignorenulls=True).over(w_moved_at_month_by_warehouse_2).alias("first_known_warehouse_opened_on"),
        F.first("moved_at").over(w_moved_at_month_by_warehouse_3).alias("first_moved_at"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_all_by_unit_price)).alias("days_since_prev_warehouse_opened_on"),
        F.round(F.cume_dist().over(w_operator_sku_id_by_warehouse), 3).alias("warehouse_opened_on_cume_dist"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_year).alias("rows_in_warehouse_opened_on_year"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_full).alias("second_unit_price"),
        F.row_number().over(w_operator_sku_id_by_warehouse).alias("warehouse_opened_on_seq"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.min("warehouse_capacity").over(w_first_known_warehouse_opened).alias("first_known_warehouse_opened_on_month_sku_id_min"),
        F.round(F.avg("unit_price_avg_3_prior").over(w_first_known_warehouse_opened), 2).alias("first_known_warehouse_opened_on_month_sku_id_avg"),
        F.lead("next_warehouse_capacity", 2).over(w_sku_id_operator_by_second_mo).alias("next2_next_warehouse_capacity"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "operator_ref",
        "warehouse_opened_on",
        "next_warehouse_capacity",
        "warehouse_opened_on_year_sum_warehouse_capacity",
        "warehouse_opened_on_year_max_unit_price",
        "unit_price_avg_3_prior",
        "warehouse_capacity_cume_dist",
        "warehouse_opened_on_cume_dist",
        "first_known_warehouse_opened_on_month_sku_id_min",
        "first_known_warehouse_opened_on_month_sku_id_avg",
        "next2_next_warehouse_capacity",
        F.abs(F.col("warehouse_capacity") - 11005).alias("warehouse_capacity_distance"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
    .orderBy(F.col("operator").desc_nulls_first())
)
