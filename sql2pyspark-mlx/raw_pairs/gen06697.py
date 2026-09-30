from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy(F.to_date("sale_date_period"), "store_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w_grp = Window.orderBy("next_revenue", "id")
w_grp_2 = Window.partitionBy("product_id").orderBy("next_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id")
w6 = Window.partitionBy("product_id").orderBy("next_revenue_bucket", "id")
w7 = Window.partitionBy("product_id").orderBy("next_revenue_bucket", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("next_revenue", "id")
w9 = Window.orderBy("product_id_min_last_known_next_revenue", "id").rowsBetween(-3, -1)
w_hist_2 = Window.orderBy(F.col("product_id_max_units").desc())
w11 = Window.partitionBy("product_id").orderBy("product_id_max_units", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5]))
    .select("*", F.date_trunc("year", "sale_date").alias("sale_date_period"))
)

metrics = (
    valid
    .filter(F.col("revenue") < 1000)
    .select(
        "id",
        "product_id",
        "units",
        F.lead("revenue", 2, 0).over(w).alias("next_revenue"),
        F.avg("revenue").over(w2).alias("revenue_avg_5_prior"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "product_id",
        "units",
        "next_revenue",
        F.ntile(10).over(w_grp).alias("next_revenue_bucket"),
        F.last("next_revenue", ignorenulls=True).over(w_grp_2).alias("last_known_next_revenue"),
        F.max("units").over(w_running).alias("product_id_max_units"),
    )
)

with_windows = (
    enriched
    .withColumn("next_units", F.lead("units", 2).over(w6))
    .withColumn("product_id_stddev_samp_units", F.stddev_samp("units").over(w_running))
    .withColumn("units_avg_6", F.avg("units").over(w7))
    .withColumn("product_id_min_last_known_next_revenue", F.min("last_known_next_revenue").over(w_running))
)

metrics_store_sales = (
    with_windows
    .select(
        "id",
        "product_id",
        "next_revenue",
        "next_revenue_bucket",
        "product_id_max_units",
        "product_id_min_last_known_next_revenue",
        F.ntile(4).over(w_hist).alias("next_revenue_bucket_2"),
        F.avg("product_id_max_units").over(w9).alias("product_id_max_units_avg_3_prior"),
        F.cume_dist().over(w_hist_2).alias("product_id_max_units_cume_dist"),
    )
)

result = (
    metrics_store_sales
    .select(
        "id",
        "product_id",
        "next_revenue",
        "product_id_min_last_known_next_revenue",
        F.max("product_id_max_units_avg_3_prior").over(w11).alias("last7_max_product_id_max_units_avg_3_prior"),
        F.min("product_id_max_units_avg_3_prior").over(w11).alias("last7_min_product_id_max_units_avg_3_prior"),
        F.sum("product_id_max_units_avg_3_prior").over(w11).alias("last7_sum_product_id_max_units_avg_3_prior"),
        F.last("product_id_min_last_known_next_revenue", ignorenulls=True).over(w_grp_2).alias("last_known_product_id_min_last_known_next_revenue"),
    )
)
