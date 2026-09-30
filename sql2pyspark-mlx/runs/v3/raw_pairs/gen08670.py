from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .union(df_products.filter(F.col("cost") > 550).select("cost"))
    .orderBy("revenue")
)
