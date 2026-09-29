from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue_filled", "id")
w3 = Window.orderBy("next_revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_avg_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("revenue") > 2600)
    .select("*", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)

scored = (
    cleaned
    .select(
        "id",
        "sale_date",
        "revenue_filled",
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.count("revenue").over(w).alias("running_count_revenue"),
        F.lead("revenue_filled", 2).over(w2).alias("next_revenue_filled"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.first("running_count_revenue", ignorenulls=True).over(w3).alias("first_known_running_count_revenue"),
        F.first("running_count_revenue", ignorenulls=True).over(w4).alias("first_known_running_count_revenue_2"),
        F.sum("running_avg_revenue").over(w5).alias("cumulative_running_avg_revenue"),
    )
)

filtered = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-02-27").cast("date"))
    .select(
        "id",
        "sale_date",
        "running_sum_revenue",
        "running_avg_revenue",
        "running_count_revenue",
        "first_known_running_count_revenue",
        "first_known_running_count_revenue_2",
        "cumulative_running_avg_revenue",
        F.round("cumulative_running_avg_revenue", 0).alias("cumulative_running_avg_revenue_rounded"),
        F.coalesce("cumulative_running_avg_revenue", F.lit(0)).alias("cumulative_running_avg_revenue_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("running_sum_revenue") >= 15200, "HIGH").otherwise("LOW").alias("running_sum_revenue_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_running_avg_revenue", [0.25, 0.5, 0.75]).alias("cumulative_running_avg_revenue_quartiles"),
        F.percentile_approx("first_known_running_count_revenue", [0.25, 0.5, 0.75]).alias("first_known_running_count_revenue_quartiles"),
    )
    .orderBy(F.col("running_sum_revenue_tier").desc())
)
