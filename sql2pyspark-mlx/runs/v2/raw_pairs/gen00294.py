from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .select("product_name")
    .unionByName(df_products.filter(F.col("cost") >= 400).select("product_name"))
    .distinct()
    .orderBy(F.col("product_name").desc())
)
