from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 9]) & (F.year("sale_date") >= 2024))
    .withColumn("least_revenue_units", F.least("revenue", "units"))
)

summary = (
    base
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    summary
    .filter(F.col("stddev_units") > 34)
    .orderBy(F.col("sale_month").desc())
)
