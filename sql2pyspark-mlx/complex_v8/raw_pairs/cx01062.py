from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on = Window.orderBy(F.col("warehouse_opened_on").desc_nulls_first(), "id")
w_warehouse_opened_on_month = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"))

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

tagged = (
    combined_details
    .filter(F.col("warehouse_opened_on").isNotNull())
    .select(
        "id",
        "operator",
        "warehouse_capacity",
        "warehouse_opened_on",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.dense_rank().over(w_all_by_warehouse_opened_on).alias("warehouse_opened_on_dense_rank"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_opened_on_month), 2).alias("warehouse_capacity_vs_warehouse_opened_on_month_"),
        F.lag("warehouse_capacity", 2).over(w_all_by_warehouse_opened_on).alias("prev2_warehouse_capacity"),
        F.round(F.avg("warehouse_capacity").over(w_warehouse_opened_on_month), 2).alias("warehouse_opened_on_month_avg_warehouse_capacity"),
    )
)

result = (
    with_trends
    .filter(F.col("warehouse_capacity_vs_warehouse_opened_on_month_").between(-10000.0, 14000.0))
    .select(
        "id",
        "operator",
        "warehouse_capacity",
        "warehouse_opened_on_dense_rank",
        "warehouse_capacity_vs_warehouse_opened_on_month_",
        "prev2_warehouse_capacity",
        "warehouse_opened_on_month_avg_warehouse_capacity",
        F.least(F.col("warehouse_capacity"), F.col("prev2_warehouse_capacity")).alias("least_warehouse_capacity_prev2_warehouse_capacit"),
        (F.col("prev2_warehouse_capacity") - F.col("warehouse_capacity")).alias("prev2_warehouse_capacity_minus_warehouse_capacit"),
    )
    .orderBy("warehouse_opened_on_month_avg_warehouse_capacity")
)
