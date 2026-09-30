from pyspark.sql import functions as F

valid_products = df_products.filter((F.size("tags") >= 1) | F.col("price").between(370, 1020))

brand_summary = (
    valid_products
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("cnt"),
    )
)

result = (
    brand_summary
    .select("brand", "min_cost", "cnt")
    .orderBy(F.col("min_cost").desc(), "brand")
    .limit(10)
)
