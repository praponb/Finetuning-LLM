from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.avg("cost").alias("avg_cost"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
    .filter(F.col("initech_count") < 4)
    .drop("initech_count")
)
