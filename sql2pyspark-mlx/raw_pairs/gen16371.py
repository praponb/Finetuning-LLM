from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(27, 29))
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_products"))
    .filter(F.col("unique_products") < 9)
    .select("store_id", "unique_products")
)
