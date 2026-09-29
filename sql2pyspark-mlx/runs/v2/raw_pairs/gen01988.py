from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(11, 15) & (F.col("units") <= 33))
    .select("id", "sale_date", "product_id", "units")
    .orderBy("sale_date")
)
