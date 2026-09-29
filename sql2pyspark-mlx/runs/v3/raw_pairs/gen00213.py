from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", "sale_date")
    .orderBy("revenue")
)
