from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-02-01")
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
    .orderBy(F.col("min_units").desc(), "store_id")
    .limit(10)
)
