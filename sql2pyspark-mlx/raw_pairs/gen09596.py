from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 9]))
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 17, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_units") < 17)
)
