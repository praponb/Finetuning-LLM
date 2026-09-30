from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") == 2023) & F.col("store_id").isin([1, 6]))
    .groupBy("store_id", "product_id")
    .agg(F.max("units").alias("max_units"))
)
