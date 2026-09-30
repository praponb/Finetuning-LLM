from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("units") >= 44, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
)
