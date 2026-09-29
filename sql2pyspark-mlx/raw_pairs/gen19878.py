from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("year_start", F.date_trunc("YEAR", "sale_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .cube("year_start", "week_start")
    .agg(
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("units").alias("min_units"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.grouping("year_start").alias("gid"),
    )
    .filter(F.col("min_units") > 0)
    .orderBy(F.col("avg_units").desc_nulls_first())
)
