from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("greatest_revenue_units", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy(F.year("sale_date")).orderBy("units", "id")
w4 = Window.partitionBy("store_id").orderBy("prev_units", "id")
w_running = Window.partitionBy("store_id", "product_id").orderBy("greatest_revenue_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy(F.col("revenue_avg_6_prior").desc_nulls_first())
w7 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.date_trunc("month", "sale_date"), "store_id").orderBy("revenue_avg_6_prior_percent_rank", "id")
w9 = Window.orderBy("event_seq", "id")
w10 = Window.orderBy(F.col("revenue_avg_6_prior_percent_rank").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_6_prior"),
        F.avg("revenue").over(w2).alias("revenue_avg_2"),
        F.lag("units", 2).over(w_hist).alias("prev_units"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue_avg_6_prior",
        "prev_units",
        F.max("revenue_avg_2").over(w_running).alias("running_max_revenue_avg_2"),
        F.count("revenue_avg_2").over(w_running).alias("running_count_revenue_avg_2"),
        F.sum("revenue_avg_2").over(w_running).alias("running_sum_revenue_avg_2"),
        F.percent_rank().over(w_grp).alias("revenue_avg_6_prior_percent_rank"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "sale_date",
        "units",
        "revenue_avg_6_prior",
        "running_max_revenue_avg_2",
        "running_sum_revenue_avg_2",
        "revenue_avg_6_prior_percent_rank",
        F.first("running_count_revenue_avg_2", ignorenulls=True).over(w7).alias("first_known_running_count_revenue_avg_2"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "sale_date",
        F.ntile(10).over(w9).alias("event_seq_bucket"),
        F.ntile(10).over(w10).alias("revenue_avg_6_prior_percent_rank_bucket"),
    )
)
