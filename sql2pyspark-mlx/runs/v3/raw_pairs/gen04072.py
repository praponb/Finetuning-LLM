from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2023-08-01")
    .select("sale_date")
    .union(df_products.select("launch_date"))
    .orderBy("sale_date")
)
