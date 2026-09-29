from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("units_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("units_rounded", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("running_min_revenue", "id")
w5 = Window.orderBy(F.col("units_rounded").desc())
w6 = Window.orderBy("last7_count_running_avg_revenue", "id")

base = (
    df_store_sales
    .filter(F.col("units").between(20, 32))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.round("units", 2).alias("units_rounded"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    latest
    .filter(F.col("sale_date") >= F.lit("2023-02-21").cast("date"))
    .withColumn("days_since_sale_date_minus_units_rounded", (F.col("days_since_sale_date") - F.col("units_rounded")))
)

scored = (
    cleaned
    .filter(F.col("days_since_sale_date") == 46)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units_rounded",
        F.count("revenue").over(w_grp).alias("running_count_revenue"),
        F.avg("revenue").over(w_grp).alias("running_avg_revenue"),
        F.min("revenue").over(w_grp).alias("running_min_revenue"),
        F.max("revenue").over(w_grp).alias("running_max_revenue"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "units_rounded",
        "running_min_revenue",
        F.min("running_avg_revenue").over(w_recent).alias("last7_min_running_avg_revenue"),
        F.max("running_avg_revenue").over(w_recent).alias("last7_max_running_avg_revenue"),
        F.sum("running_avg_revenue").over(w_recent).alias("last7_sum_running_avg_revenue"),
        F.count("running_avg_revenue").over(w_recent).alias("last7_count_running_avg_revenue"),
        F.lead("running_avg_revenue", 2).over(w4).alias("next_running_avg_revenue"),
    )
)

scored_store_sales = (
    metrics
    .withColumn("units_rounded_cume_dist", F.cume_dist().over(w5))
    .withColumn("prev_next_running_avg_revenue", F.lag("next_running_avg_revenue", 2).over(w6))
)

result = (
    scored_store_sales
    .groupBy(
        F.when(F.col("next_running_avg_revenue") >= 4400, "HIGH").otherwise("LOW").alias("next_running_avg_revenue_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.avg("last7_min_running_avg_revenue").alias("avg_last7_min_running_avg_revenue"),
        F.round(F.avg("units_rounded_cume_dist"), 2).alias("avg_units_rounded_cume_dist"),
    )
    .orderBy(F.col("next_running_avg_revenue_tier").desc_nulls_last())
)
