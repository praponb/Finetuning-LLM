from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
    .orderBy("avg_price", F.col("category").desc())
    .limit(5)
)
