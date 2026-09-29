from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("distinct_store_ids").desc())
)
