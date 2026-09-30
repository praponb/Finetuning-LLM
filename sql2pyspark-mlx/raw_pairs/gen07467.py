from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.month("sale_date"), "store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("running_avg_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("first_known_revenue_per_unit", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("week_start").orderBy(F.col("cnt").desc())

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-02")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("revenue").over(w_ordered).alias("running_avg_revenue"),
        F.sum("revenue").over(w_ordered).alias("running_sum_revenue"),
        F.count("revenue").over(w_ordered).alias("running_count_revenue"),
        F.first("revenue_per_unit", ignorenulls=True).over(w_grp).alias("first_known_revenue_per_unit"),
    )
)

base = (
    with_windows
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-04-18"))) & (F.col("running_sum_revenue") < 14000))
    .withColumn("greatest_units_running_sum_revenue", F.greatest("units", "running_sum_revenue"))
    .withColumn("running_sum_revenue_rounded", F.round("running_sum_revenue", 2))
)

with_windows_store_sales = (
    base
    .filter(F.col("revenue_per_unit") < 48)
    .select(
        "*",
        F.sum("revenue_per_unit").over(w3).alias("running_revenue_per_unit"),
        F.avg("revenue_per_unit").over(w4).alias("revenue_per_unit_avg_3_prior"),
    )
)

summary = (
    with_windows_store_sales
    .groupBy(
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.when(F.col("first_known_revenue_per_unit") >= 74, "HIGH").otherwise("LOW").alias("first_known_revenue_per_unit_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("revenue_per_unit_avg_3_prior", [0.25, 0.5, 0.75]).alias("revenue_per_unit_avg_3_prior_quartiles"),
        F.avg("running_sum_revenue").alias("avg_running_sum_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w5))
)

result = (
    summary
    .select(
        "week_start",
        "first_known_revenue_per_unit_tier",
        "quarter_start",
        "revenue_per_unit_avg_3_prior_quartiles",
        "cnt_percent_rank",
    )
)
