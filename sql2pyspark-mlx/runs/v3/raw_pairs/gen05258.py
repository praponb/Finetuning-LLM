from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Acme")
    .select("launch_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
    .orderBy("launch_date")
)
