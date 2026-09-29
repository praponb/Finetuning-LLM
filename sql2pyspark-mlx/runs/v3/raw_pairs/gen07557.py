from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)
