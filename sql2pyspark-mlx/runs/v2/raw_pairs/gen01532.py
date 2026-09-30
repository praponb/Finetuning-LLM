from pyspark.sql import functions as F

max_launch_date = df_products.agg(F.max("launch_date")).first()[0]

result = (
    df_products
    .filter(F.col("launch_date") == max_launch_date)
    .select("id", "launch_date", "cost")
)
