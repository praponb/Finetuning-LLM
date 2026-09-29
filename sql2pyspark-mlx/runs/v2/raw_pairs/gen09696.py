from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull() & F.array_contains("tags", "new") & F.col("stock_qty").isNotNull())
    .withColumn("category_product_name", F.concat("category", F.lit("/"), "product_name"))
)
