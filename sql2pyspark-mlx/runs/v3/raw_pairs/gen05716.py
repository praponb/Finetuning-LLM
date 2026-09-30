from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("cost").between(270, 450))
    .groupBy("brand")
    .agg(
        F.sum("price").alias("total_price"),
        F.countDistinct("price").alias("unique_brands"),
    )
    .orderBy(F.col("total_price").desc())
)
