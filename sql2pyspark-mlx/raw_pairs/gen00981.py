from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", "product_name")
    .orderBy(F.col("brand").desc(), "id")
)
