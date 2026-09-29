from pyspark.sql import functions as F

min_launch_date = df_products.agg(F.min("launch_date")).first()[0]

result = (
    df_products
    .filter(F.col("launch_date") == min_launch_date)
    .select("id", "launch_date", "cost", "product_name")
)
