from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
    .orderBy(F.col("home_count").desc())
)
