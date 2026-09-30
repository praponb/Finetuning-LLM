from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%wireless%"))
    .select("id", "price", F.sort_array("tags").alias("sorted_tags"))
    .orderBy("price", "id")
    .limit(5)
)
