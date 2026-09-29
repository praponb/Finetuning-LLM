from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy(F.date_trunc("week", "launch_date"), "store_id").orderBy(F.col("prev_revenue_per_unit").desc_nulls_last())
w5 = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id")
w_hist = Window.partitionBy("product_id", "store_id").orderBy("prev_revenue_per_unit", "id").rowsBetween(-5, -1)

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-24").cast("date")) & F.month("sale_date").isin([1, 4, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        F.ceil("revenue").alias("revenue_whole"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.max("revenue_whole").over(w).alias("store_id_max_revenue_whole"),
        F.lag("revenue_per_unit", 2).over(w2).alias("prev_revenue_per_unit"),
        F.avg("units").over(w3).alias("units_avg_2_prior"),
    )
)

filtered = (
    enriched
    .filter(F.col("product_id").isin([3, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue_per_unit",
        "prev_revenue_per_unit",
        "units_avg_2_prior",
        F.least("revenue_per_unit", "prev_revenue_per_unit").alias("least_revenue_per_unit_prev_revenue_per_unit"),
    )
)

enriched_store_sales = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.revenue_per_unit",
        "a.prev_revenue_per_unit",
        "a.least_revenue_per_unit_prev_revenue_per_unit",
        F.col("d.cost"),
        F.col("d.launch_date"),
        F.col("d.price"),
    )
)

scored = (
    enriched_store_sales
    .withColumn("prev_revenue_per_unit_percent_rank", F.percent_rank().over(w_running))
    .withColumn("revenue_per_unit_bucket", F.ntile(4).over(w5))
    .withColumn("revenue_per_unit_avg_5_prior", F.avg("revenue_per_unit").over(w_hist))
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.max("prev_revenue_per_unit").alias("max_prev_revenue_per_unit"),
        F.max("revenue_per_unit").alias("max_revenue_per_unit"),
    )
    .filter(F.col("max_revenue_per_unit") < 1000000)
    .orderBy(F.col("total_cost").desc())
)
