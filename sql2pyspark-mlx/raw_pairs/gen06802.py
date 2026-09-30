from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2024)
    .select("launch_date")
    .unionByName(df_products.filter(F.col("stock_qty") <= 226).select("launch_date"))
    .distinct()
)
