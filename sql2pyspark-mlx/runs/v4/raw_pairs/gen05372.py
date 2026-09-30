from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("price").between(920, 980))
    .withColumn("product_name_category_key", F.concat_ws("/", "product_name", "category"))
)

result = (
    filtered
    .groupBy("supplier_id")
    .pivot("category", ["Sports", "Books", "Home"])
    .agg(F.sum("price"))
    .select(
        "supplier_id",
        F.col("Sports").alias("sports"),
        F.col("Books").alias("books"),
        F.col("Home").alias("home"),
    )
)
