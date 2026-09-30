from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 7, 8]))
    .groupBy("brand")
    .agg(
        F.count("price").alias("price_count"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
    .orderBy(F.col("brand").desc())
    .limit(5)
)
