from pyspark.sql import functions as F

max_launch_date = df_products.filter(F.col("supplier_id").isin([10, 27, 37])).agg(F.max("launch_date")).first()[0]

result = (
    df_products
    .filter(F.col("launch_date") == max_launch_date)
    .select("id", "launch_date", "category", "price")
)
