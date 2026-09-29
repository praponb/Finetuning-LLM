from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("qty").desc(), "id")
w_warehouse_opened_on_month = Window.partitionBy(F.month("warehouse_opened_on"))
w_warehouse_id_by_moved_at_yea = Window.partitionBy("warehouse_id").orderBy("moved_at_year", "id")
w_warehouse_id_by_moved_at_yea_2 = Window.partitionBy("warehouse_id").orderBy("moved_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_wareh = Window.partitionBy("warehouse_id", "sku_id").orderBy("warehouse_opened_on", "id").rowsBetween(-2, -1)

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

combined_details_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

with_trends = (
    combined_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "moved_at_year",
        "warehouse_opened_on",
        F.ntile(3).over(w_warehouse_opened_on_month_by).alias("qty_tercile"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_opened_on_month), 2).alias("qty_vs_warehouse_opened_on_month_avg"),
        (F.col("qty") - F.lag("qty").over(w_warehouse_opened_on_month_by)).alias("qty_change"),
        F.ntile(5).over(w_warehouse_id_by_moved_at_yea).alias("moved_at_year_quintile"),
        F.size(F.collect_set("move_type").over(w_warehouse_opened_on_month)).alias("distinct_move_type_in_warehouse_opened_on_month"),
        F.nth_value("unit_price", 2).over(w_warehouse_id_by_moved_at_yea_2).alias("second_unit_price"),
        F.max("unit_price").over(w_warehouse_opened_on_month).alias("warehouse_opened_on_month_max_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w_warehouse_opened_on_month_by_2).alias("first_known_unit_price"),
        F.first("unit_price").over(w_warehouse_id_by_moved_at_yea_2).alias("first_unit_price"),
        F.sum("qty").over(w_warehouse_id_sku_id_by_wareh).alias("qty_sum_2_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "moved_at_year",
        "warehouse_opened_on",
        "qty_tercile",
        "qty_vs_warehouse_opened_on_month_avg",
        "moved_at_year_quintile",
        "second_unit_price",
        "first_unit_price",
        F.round((F.unix_timestamp(F.col("moved_at")) - F.unix_timestamp(F.col("moved_at_year"))) / 3600, 1).alias("hours_moved_at_year_to_moved_at"),
    )
    .orderBy("qty_vs_warehouse_opened_on_month_avg", F.col("qty_tercile").desc())
)
