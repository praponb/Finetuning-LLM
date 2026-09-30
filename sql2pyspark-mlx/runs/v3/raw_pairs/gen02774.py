from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([8, 25]))
    .withColumn("product_name_category", F.concat("product_name", F.lit("/"), "category"))
    .orderBy("supplier_id", "brand")
)
