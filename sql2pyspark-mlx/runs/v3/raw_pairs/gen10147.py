from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter((F.col("revenue") <= 3200) & F.col("store_id").isin([1, 5]))

summary = (
    valid_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = summary
