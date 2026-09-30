from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 1900)
    .select("product_id", "sale_date", "units")
    .orderBy("sale_date")
)
