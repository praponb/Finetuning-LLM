from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2023-09-09") | F.col("product_id").isin([2, 3, 6]))
    .select("units", "revenue")
    .orderBy("units")
)
