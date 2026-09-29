from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .select("sale_date")
    .unionByName(df_store_sales.filter(F.year("sale_date") >= 2023).select("sale_date"))
)
