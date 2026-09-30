from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 5, 7]) & ((F.col("revenue") > 2100) | F.col("sale_date").between("2023-05-01", "2023-08-01")))
    .groupBy("store_id", "product_id", F.year("sale_date").alias("sale_year"))
    .agg(F.max("sale_date").alias("last_sale_date"))
)
