from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3500, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
    )
)
