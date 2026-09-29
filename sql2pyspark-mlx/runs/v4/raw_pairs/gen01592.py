from pyspark.sql import functions as F

result = (
    df_store_sales
    .cube(
        F.when(F.col("revenue") >= 3900, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.stddev_samp("units").alias("units_stddev"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
        F.round(F.avg("units"), 2).alias("avg_units_3"),
    )
    .filter(F.col("units_stddev") >= 0)
    .orderBy(F.col("avg_units_2").desc_nulls_last())
)
