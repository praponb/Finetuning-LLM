from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Smar%"))
    .groupBy("category")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("product_count"),
    )
    .orderBy("min_cost")
)
