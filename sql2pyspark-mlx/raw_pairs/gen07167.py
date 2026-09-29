from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.avg("price").alias("avg_price"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
    )
)
