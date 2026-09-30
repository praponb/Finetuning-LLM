from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.stddev_samp("units").alias("units_stddev"),
    )
    .filter(F.col("units_stddev") >= 0)
    .orderBy("units_stddev")
)
