from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w_running = Window.partitionBy("store_id")
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "store_id")
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sale_date", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("cumulative_revenue").asc_nulls_last(), "id")
w8 = Window.partitionBy("store_id").orderBy("corr_units_revenue", "id")
w9 = Window.partitionBy("store_id").orderBy("revenue", "id")
w10 = Window.partitionBy("store_id").orderBy(F.col("cumulative_revenue").desc())
w11 = Window.orderBy(F.col("cumulative_revenue").desc_nulls_last())
w12 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("cumulative_revenue_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("corr_units_revenue", "id")
w15 = Window.partitionBy("product_id")
w16 = Window.orderBy("cumulative_revenue_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .filter(F.col("product_id") == 10)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.corr("units", "revenue").over(w_running).alias("corr_units_revenue"),
        F.min("revenue").over(w3).alias("sale_date_period_min_revenue"),
        F.sum("revenue").over(w4).alias("cumulative_revenue"),
        F.sum("revenue").over(w5).alias("running_revenue"),
        F.lag("units", 2).over(w6).alias("prev_units"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored_store_sales = (
    latest
    .select(
        "id",
        "product_id",
        "sale_date",
        "corr_units_revenue",
        "sale_date_period_min_revenue",
        "cumulative_revenue",
        "running_revenue",
        F.lag("prev_units", 2).over(w8).alias("prev_prev_units"),
        F.lag("prev_units", 1).over(w9).alias("prev_prev_units_2"),
        F.percent_rank().over(w10).alias("cumulative_revenue_percent_rank"),
        F.cume_dist().over(w11).alias("cumulative_revenue_cume_dist"),
    )
)

metrics = (
    scored_store_sales
    .select(
        "*",
        F.first("prev_prev_units_2", ignorenulls=True).over(w12).alias("first_known_prev_prev_units_2"),
        F.sum("sale_date_period_min_revenue").over(w13).alias("cumulative_sale_date_period_min_revenue"),
        F.lead("cumulative_revenue", 1).over(w14).alias("next_cumulative_revenue"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "sale_date",
        "corr_units_revenue",
        "cumulative_revenue",
        "running_revenue",
        "prev_prev_units_2",
        "cumulative_revenue_percent_rank",
        "cumulative_revenue_cume_dist",
        "first_known_prev_prev_units_2",
        "next_cumulative_revenue",
        F.round((F.col("cumulative_revenue") / F.sum("cumulative_revenue").over(w15)), 4).alias("cumulative_revenue_share"),
        F.sum("sale_date_period_min_revenue").over(w16).alias("running_sale_date_period_min_revenue"),
    )
)
