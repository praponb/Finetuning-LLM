from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("units").between(18, 34))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 6))
)

agg = (
    valid_store_sales
    .groupBy("product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    agg
    .filter(F.col("stddev_units") > 64)
    .select("product_id", "stddev_units")
    .orderBy("product_id")
    .limit(10)
)
