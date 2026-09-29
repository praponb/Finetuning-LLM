from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "product_id").orderBy(F.col("units").asc_nulls_first())
w2 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("units", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.month("sale_date"), "product_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("units_bucket_rounded").desc_nulls_last(), "id")
w5 = Window.partitionBy("store_id").orderBy("units_bucket", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("units_rank").desc(), "id")

metrics = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.rank().over(w).alias("units_rank"),
        F.avg("revenue").over(w2).alias("revenue_avg_3_prior"),
        F.ntile(10).over(w3).alias("units_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("sale_date") >= F.lit("2023-10-02").cast("date"))
    .withColumn("units_bucket_rounded", F.round("units_bucket", 1))
    .withColumn("units_rank_num", F.col("units_rank").cast("double"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .select(
        "id",
        "store_id",
        "sale_date",
        "units_rank",
        "units_bucket",
        "units_rank_num",
        F.avg("revenue_avg_3_prior").over(w5).alias("last7_avg_revenue_avg_3_prior"),
        F.max("revenue_avg_3_prior").over(w5).alias("last7_max_revenue_avg_3_prior"),
        F.min("revenue_avg_3_prior").over(w5).alias("last7_min_revenue_avg_3_prior"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("last7_max_revenue_avg_3_prior_tier", F.when(F.col("last7_max_revenue_avg_3_prior") >= 3600, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .groupingSets(
        [["last7_max_revenue_avg_3_prior_tier", "week_start"], ["last7_max_revenue_avg_3_prior_tier"]],
        "last7_max_revenue_avg_3_prior_tier",
        "week_start",
    )
    .agg(
        F.percentile_approx("units_bucket", [0.25, 0.5, 0.75]).alias("units_bucket_quartiles"),
        F.percentile_approx("units_rank", 0.9).alias("units_rank_p90"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
