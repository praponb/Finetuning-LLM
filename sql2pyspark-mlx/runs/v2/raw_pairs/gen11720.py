from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("is_premium", F.array_contains("tags", "premium"))
    .orderBy(F.col("product_name").desc(), "id")
    .limit(10)
)
