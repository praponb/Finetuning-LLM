from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("brand").isNull() & F.col("brand").isin(["Initech", "Stark", "Globex"]))
    .withColumn("category_length", F.length("category"))
)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(
        F.max("cost").alias("max_cost"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = products_stats.orderBy(F.col("max_cost").desc())
