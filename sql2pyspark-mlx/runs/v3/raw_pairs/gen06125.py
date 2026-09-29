from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 12) & (F.col("product_id") == 2))
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy(F.col("stddev_units").desc())
)
