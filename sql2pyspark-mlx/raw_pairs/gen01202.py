from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") < 12)
    .select("product_id", "min_units")
    .orderBy(F.col("min_units").desc())
)
