from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") == 37) & (F.col("revenue") > 1800) & F.col("product_id").isin([2, 5]))
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.max("units").alias("max_units"))
    .orderBy(F.col("product_id").desc())
)
