from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() | F.array_contains("tags", "sale"))
    .withColumn("category_brand", F.concat("category", F.lit(", "), "brand"))
    .withColumn("margin", (F.col("cost") * F.col("price")))
    .orderBy("product_name", "id")
    .limit(5)
)
