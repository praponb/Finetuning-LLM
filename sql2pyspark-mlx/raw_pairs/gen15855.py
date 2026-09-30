from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 580)
    .select("product_name")
    .unionByName(df_products.filter(F.col("cost") <= 460).select("product_name"))
    .distinct()
)
