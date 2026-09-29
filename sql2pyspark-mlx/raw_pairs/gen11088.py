from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 830)
    .select("product_name")
    .union(df_transactions.select("merchant"))
    .distinct()
    .orderBy("product_name")
)
