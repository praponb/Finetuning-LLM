from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_warehouse_city_by_moved_at_m = Window.partitionBy("warehouse_city").orderBy(F.col("moved_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_by_unit_price = Window.partitionBy("moved_at_month").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_moved_at_month_by_unit_price_2 = Window.partitionBy("moved_at_month").orderBy(F.col("unit_price").asc_nulls_last())
w_sku_category_operator_by_uni = Window.partitionBy("sku_category", "operator").orderBy("unit_price", "id").rowsBetween(-1, -1)
w_warehouse_city_by_unit_price = Window.partitionBy("warehouse_city").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_sku_category = Window.partitionBy("moved_at_month", "sku_category")
w_sku_category_operator_by_uni_2 = Window.partitionBy("sku_category", "operator").orderBy("unit_price")

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
    )
)

with_details = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("sk.category") == "Electronics")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.moved_at_month",
        F.col("sk.category").alias("sku_category"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_warehouse_city_by_moved_at_m).alias("last_known_warehouse_opened_on"),
        F.sum("unit_price").over(w_moved_at_month_by_unit_price).alias("unit_price_sum_2"),
        F.dense_rank().over(w_moved_at_month_by_unit_price_2).alias("unit_price_dense_rank"),
        F.round(F.avg("moved_at_month").over(w_sku_category_operator_by_uni), 2).alias("moved_at_month_avg_1_prior"),
        F.max("moved_at_month").over(w_warehouse_city_by_unit_price).alias("max_moved_at_month_so_far"),
        F.first("moved_at_month", ignorenulls=True).over(w_warehouse_city_by_unit_price).alias("first_known_moved_at_month"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_month_sku_category), 2).alias("unit_price_vs_moved_at_month_sku_category_avg"),
        F.round(F.percent_rank().over(w_sku_category_operator_by_uni_2), 3).alias("unit_price_percent_rank"),
    )
)

derived = (
    sequenced
    .withColumn("days_since_warehouse_opened_on", F.datediff(F.lit("2025-12-31").cast("date"), F.col("warehouse_opened_on")))
)

result = (
    derived
    .select(
        "id",
        "operator",
        "unit_price",
        "moved_at_month",
        "sku_category",
        "warehouse_city",
        "distinct_move_type_in_sku_id",
        "last_known_warehouse_opened_on",
        "unit_price_sum_2",
        "unit_price_dense_rank",
        "moved_at_month_avg_1_prior",
        "unit_price_vs_moved_at_month_sku_category_avg",
        "unit_price_percent_rank",
        "days_since_warehouse_opened_on",
    )
    .orderBy("sku_category")
)
