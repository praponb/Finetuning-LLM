from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
    )
    .filter(F.col("initech_count") > 44)
    .orderBy(F.col("unique_brands").desc())
)
