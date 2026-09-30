from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.min("sale_date").alias("first_sale_date"),
    )
)
