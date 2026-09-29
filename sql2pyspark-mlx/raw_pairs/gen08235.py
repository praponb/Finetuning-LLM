from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 33, "LARGE").otherwise("SMALL").alias("units_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.stddev_samp("units").alias("units_stddev"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
    )
)
