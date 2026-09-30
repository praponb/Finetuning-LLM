from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") >= 24)
    .select("product_id", "min_units")
)
