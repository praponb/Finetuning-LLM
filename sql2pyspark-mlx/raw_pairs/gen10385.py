from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "store_id")
w2 = Window.orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("row_count").desc())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-10-05").cast("date"))
    .select("*", (F.col("units") - F.col("revenue")).alias("revenue_per_unit"))
)

with_windows = (
    filtered
    .filter(F.col("store_id").isin([1, 2, 5]))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w)), 4))
    .withColumn("units_bucket", F.ntile(4).over(w2))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"))
)

metrics = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_per_unit",
        "units_bucket",
        "opened_date",
        F.max("revenue").over(w3).alias("running_max_revenue"),
        F.avg("revenue").over(w3).alias("running_avg_revenue"),
        F.sum("revenue").over(w3).alias("running_sum_revenue"),
        F.min("revenue").over(w3).alias("running_min_revenue"),
    )
)

result = (
    metrics
    .groupBy(
        "product_id",
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("running_sum_revenue") >= 20900, "LARGE").otherwise("SMALL").alias("running_sum_revenue_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w4))
)
