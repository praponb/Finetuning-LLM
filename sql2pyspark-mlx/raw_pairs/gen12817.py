from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 35, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
        F.when(F.col("revenue") >= 3500, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 0)
    .orderBy(F.col("revenue_p50").desc_nulls_last())
)
