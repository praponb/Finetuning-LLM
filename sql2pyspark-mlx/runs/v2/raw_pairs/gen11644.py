from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2023-12-01")
    .select("id", "supplier_id", "category", "cost")
)
