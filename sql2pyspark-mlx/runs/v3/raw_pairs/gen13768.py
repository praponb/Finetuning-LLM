from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("price").alias("min_price"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
    )
    .orderBy(F.col("min_price").desc())
)
