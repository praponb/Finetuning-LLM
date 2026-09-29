from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("store_id") == 4)

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.countDistinct("units").alias("unique_products"),
    )
)

result = agg.select("product_id", "units_count", "unique_products")
