from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(520, 670))
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("price").alias("min_price"),
    )
    .orderBy(F.col("min_price").desc(), F.col("min_cost").desc(), "brand")
    .limit(20)
)
