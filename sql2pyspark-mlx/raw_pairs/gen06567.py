from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup("store_id", "product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.countDistinct("units").alias("unique_products"),
        F.min("sale_date").alias("min_sale_date"),
    )
)
