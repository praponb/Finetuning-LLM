from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "city").orderBy("revenue", "id")
w4 = Window.partitionBy("region", "product_id").orderBy(F.col("revenue_bucket_2").desc_nulls_first(), "id")
w_recent = Window.partitionBy("region", "product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_avg_revenue_bucket", "id")
w8 = Window.orderBy("running_min_revenue_bucket", "id")
w_hist = Window.orderBy(F.col("sqft").desc(), "id")
w10 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.orderBy("running_avg_revenue_bucket", "id").rowsBetween(-6, -1)

joined = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.opened_date").between("2024-04-01", "2025-09-01"))
    .select("s.*", F.col("d.sqft"), F.col("d.region"), F.col("d.city"))
)

enriched = (
    joined
    .select(
        "id",
        "product_id",
        "units",
        "sqft",
        "region",
        F.ntile(10).over(w).alias("revenue_bucket"),
        F.ntile(3).over(w2).alias("revenue_bucket_2"),
        F.ntile(4).over(w3).alias("revenue_bucket_3"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "sqft", "region", "revenue_bucket")
)

enriched_store_sales = (
    latest
    .select(
        "id",
        "sqft",
        F.min("revenue_bucket").over(w_recent).alias("running_min_revenue_bucket"),
        F.avg("revenue_bucket").over(w_recent).alias("running_avg_revenue_bucket"),
        F.max("revenue_bucket").over(w_recent).alias("running_max_revenue_bucket"),
        F.sum("sqft").over(w_running).alias("running_sqft"),
    )
)

scored = (
    enriched_store_sales
    .select(
        "id",
        "sqft",
        "running_min_revenue_bucket",
        "running_avg_revenue_bucket",
        "running_max_revenue_bucket",
        "running_sqft",
        F.ntile(3).over(w7).alias("running_avg_revenue_bucket_bucket"),
        F.lead("running_max_revenue_bucket", 2).over(w8).alias("next_running_max_revenue_bucket"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.avg("running_avg_revenue_bucket").over(w10).alias("running_avg_revenue_bucket_avg_6"),
        F.avg("running_min_revenue_bucket").over(w11).alias("running_min_revenue_bucket_avg_6_prior"),
    )
)

result = (
    scored
    .withColumn("running_min_revenue_bucket_tier", F.when(F.col("running_min_revenue_bucket") >= 7.1, "LARGE").otherwise("SMALL"))
    .withColumn("running_min_revenue_bucket_avg_6_prior_tier", F.when(F.col("running_min_revenue_bucket_avg_6_prior") >= 9, "PREMIUM").otherwise("STANDARD"))
    .rollup(
        "running_min_revenue_bucket_tier",
        "running_min_revenue_bucket_avg_6_prior_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("next_running_max_revenue_bucket"), 2).alias("avg_next_running_max_revenue_bucket"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_next_running_max_revenue_bucket") > 0)
    .orderBy("running_min_revenue_bucket_avg_6_prior_tier")
)
