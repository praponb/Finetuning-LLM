from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
    )
    .filter(F.col("stark_price") > 65)
)
