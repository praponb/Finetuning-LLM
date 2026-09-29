from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("week", "sale_date").alias("week_start"),
        "store_id",
        F.when(F.col("revenue") >= 4300, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
