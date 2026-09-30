from pyspark.sql import functions as F

result = (
    df_products
    .select("product_name")
    .union(df_customers.select("name"))
)
