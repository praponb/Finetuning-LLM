from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("revenue", "id")
w_ordered_2 = Window.partitionBy("product_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("revenue_bucket").desc_nulls_last())
w5 = Window.orderBy(F.col("corr_revenue_units").asc_nulls_first())
w6 = Window.partitionBy("product_id", "store_id")
w7 = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w8 = Window.partitionBy("store_id")
w9 = Window.partitionBy("product_id").orderBy("corr_revenue_units_cume_dist", "id")
w10 = Window.partitionBy("store_id", "product_id").orderBy("revenue_bucket_share", "id")
w11 = Window.partitionBy("store_id").orderBy("revenue_bucket", "id")
w12 = Window.partitionBy("store_id").orderBy("revenue_bucket_share", "id")
w_grp = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-2, -1)
w14 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("revenue_bucket_share", "id")
w16 = Window.partitionBy("corr_revenue_units_cume_dist_avg_2_prior_tier").orderBy(F.col("row_count").desc())

enriched = (
    df_store_sales
    .withColumn("running_revenue", F.sum("revenue").over(w))
    .withColumn("revenue_bucket", F.ntile(3).over(w_ordered))
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w_ordered_2))
)

enriched_store_sales = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w4).alias("revenue_bucket_dense_rank"),
        F.cume_dist().over(w5).alias("corr_revenue_units_cume_dist"),
        F.round((F.col("revenue_bucket") / F.sum("revenue_bucket").over(w_ordered_2)), 4).alias("revenue_bucket_share"),
        F.round((F.col("running_revenue") / F.sum("running_revenue").over(w6)), 4).alias("running_revenue_share"),
        F.lead("revenue", 1, 0).over(w7).alias("next_revenue"),
    )
)

with_windows = (
    enriched_store_sales
    .select(
        "*",
        F.round((F.col("revenue") / F.sum("revenue").over(w8)), 4).alias("revenue_share"),
        F.ntile(3).over(w9).alias("corr_revenue_units_cume_dist_bucket"),
    )
)

valid = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2024-05-03").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "running_revenue",
        "revenue_bucket",
        "corr_revenue_units",
        "corr_revenue_units_cume_dist",
        "revenue_bucket_share",
        "running_revenue_share",
        "next_revenue",
        "revenue_share",
        "corr_revenue_units_cume_dist_bucket",
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
        (F.col("revenue_bucket_dense_rank") * F.col("corr_revenue_units_cume_dist")).alias("revenue_bucket_dense_rank_times_corr_revenue_units_cume_dist"),
    )
)

base = (
    valid
    .filter(F.col("next_revenue") < 2100)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "running_revenue",
        "revenue_bucket",
        "corr_revenue_units",
        "corr_revenue_units_cume_dist",
        "revenue_bucket_share",
        "revenue_share",
        "corr_revenue_units_cume_dist_bucket",
        "revenue_bucket_dense_rank_times_corr_revenue_units_cume_dist",
        F.round((F.col("corr_revenue_units_cume_dist") * 0.9), 2).alias("corr_revenue_units_cume_dist_adjusted"),
    )
)

with_windows_store_sales = (
    base
    .select(
        "*",
        F.lag("corr_revenue_units_cume_dist_adjusted", 1).over(w10).alias("prev_corr_revenue_units_cume_dist_adjusted"),
        F.lead("corr_revenue_units_cume_dist_adjusted", 1).over(w11).alias("next_corr_revenue_units_cume_dist_adjusted"),
    )
)

enriched_store_sales_2 = (
    with_windows_store_sales
    .withColumn("product_id_sum_corr_revenue_units", F.sum("corr_revenue_units").over(w6))
    .withColumn("next_corr_revenue_units_cume_dist_adjusted_2", F.lead("corr_revenue_units_cume_dist_adjusted", 1).over(w12))
    .withColumn("corr_revenue_units_cume_dist_avg_2_prior", F.avg("corr_revenue_units_cume_dist").over(w_grp))
    .withColumn("last_known_corr_revenue_units_cume_dist_adjusted", F.last("corr_revenue_units_cume_dist_adjusted", ignorenulls=True).over(w14))
    .withColumn("revenue_bucket_share_bucket", F.ntile(10).over(w_recent))
)

result = (
    enriched_store_sales_2
    .groupBy(
        F.when(F.col("corr_revenue_units_cume_dist_avg_2_prior") >= 1, "PREMIUM").otherwise("STANDARD").alias("corr_revenue_units_cume_dist_avg_2_prior_tier"),
        "product_id",
        F.when(F.col("revenue_share") >= 0.8, "PREMIUM").otherwise("STANDARD").alias("revenue_share_tier"),
    )
    .agg(
        F.max("corr_revenue_units").alias("max_corr_revenue_units"),
        F.count("*").alias("row_count"),
        F.round(F.avg("corr_revenue_units_cume_dist_avg_2_prior"), 2).alias("avg_corr_revenue_units_cume_dist_avg_2_prior"),
    )
    .filter(F.col("avg_corr_revenue_units_cume_dist_avg_2_prior") > 0)
    .withColumn("row_count_rank", F.rank().over(w16))
    .orderBy("corr_revenue_units_cume_dist_avg_2_prior_tier")
)
