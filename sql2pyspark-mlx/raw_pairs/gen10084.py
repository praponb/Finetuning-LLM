from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "sale_date_plus_months"), "product_id").orderBy("sale_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("sale_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("running_revenue_2_times_units_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("sale_date"))
w6 = Window.partitionBy("product_id").orderBy("running_revenue_3", "id")

base = (
    df_store_sales
    .filter(F.col("revenue") <= 1500)
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 12))
    .withColumn("units_whole", F.floor("units"))
)

metrics = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "sale_date_plus_months",
        "units_whole",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.sum("revenue").over(w_recent).alias("running_revenue_2"),
        F.sum("revenue").over(w3).alias("running_revenue_3"),
    )
)

base_store_sales = (
    metrics
    .filter(F.col("sale_date") <= "2023-11-01")
    .select(
        "id",
        "product_id",
        "sale_date",
        "sale_date_plus_months",
        "units_whole",
        "running_revenue",
        "running_revenue_3",
        (F.col("running_revenue_2") * F.col("units_whole")).alias("running_revenue_2_times_units_whole"),
    )
)

with_dims = (
    base_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .filter(F.col("d.price") >= 450)
    .select(
        "e.id",
        "e.product_id",
        "e.sale_date",
        "e.sale_date_plus_months",
        "e.units_whole",
        "e.running_revenue",
        "e.running_revenue_3",
        "e.running_revenue_2_times_units_whole",
        F.col("d.product_name"),
    )
)

metrics_store_sales = (
    with_dims
    .select(
        "*",
        F.min("running_revenue").over(w4).alias("running_min_running_revenue"),
        F.count("running_revenue").over(w4).alias("running_count_running_revenue"),
        F.max("running_revenue").over(w4).alias("running_max_running_revenue"),
        F.avg("running_revenue").over(w4).alias("running_avg_running_revenue"),
        F.max("running_revenue_2_times_units_whole").over(w5).alias("sale_date_year_max_running_revenue_2_times_units_whole"),
    )
)

result = (
    metrics_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "sale_date_plus_months",
        "running_revenue",
        "product_name",
        "running_min_running_revenue",
        "running_count_running_revenue",
        "running_max_running_revenue",
        F.lag("running_revenue_2_times_units_whole").over(w6).alias("prev_running_revenue_2_times_units_whole"),
        F.lag("running_min_running_revenue").over(w6).alias("prev_running_min_running_revenue"),
        F.row_number().over(w6).alias("row_seq"),
    )
)
