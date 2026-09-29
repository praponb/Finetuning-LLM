from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("category")
)
