from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("sale_date"), "store_id").orderBy(F.col("running_min_units").desc(), "id")
w4 = Window.orderBy(F.col("running_max_units").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_known_units").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .filter(F.col("revenue") >= 1200)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.min("units").over(w_ordered).alias("running_min_units"),
        F.sum("units").over(w_ordered).alias("running_sum_units"),
        F.max("units").over(w_ordered).alias("running_max_units"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
    )
)

valid = (
    scored
    .filter(F.month("sale_date").isin([3, 4, 12]))
    .select(
        "*",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        F.least("running_max_units", "revenue").alias("least_running_max_units_revenue"),
    )
)

scored_store_sales = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "running_sum_units",
        "last_known_units",
        "least_running_max_units_revenue",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(5).over(w4).alias("running_max_units_bucket"),
    )
)

enriched = (
    scored_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "least_running_max_units_revenue",
        "event_seq",
        "running_max_units_bucket",
        F.ntile(10).over(w5).alias("last_known_units_bucket"),
        F.sum("least_running_max_units_revenue").over(w6).alias("running_least_running_max_units_revenue"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .groupingSets(
        [["brand", "category"], ["brand"], []],
        F.col("d.brand").alias("brand"),
        F.col("d.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.revenue")).alias("min_revenue"),
        F.round(F.avg(F.col("a.running_max_units_bucket")), 2).alias("avg_running_max_units_bucket"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("category").asc_nulls_last())
)
