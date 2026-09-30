from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.max("cost").alias("max_cost"),
        F.min("price").alias("min_price"),
    )
    .filter(F.col("max_cost") < 710)
    .orderBy("max_cost", "brand")
    .limit(10)
)
