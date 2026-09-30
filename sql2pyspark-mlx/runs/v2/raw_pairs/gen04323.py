from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2025)
    .select("id", "launch_date", "price")
)
