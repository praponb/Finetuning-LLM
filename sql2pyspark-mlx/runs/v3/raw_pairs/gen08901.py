from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("units", "revenue", "product_id")
    .orderBy("product_id")
)
