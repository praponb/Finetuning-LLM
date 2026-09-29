from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Smart%") & (F.col("launch_date") < "2025-02-05"))
    .select("id", "cost", "price")
)
