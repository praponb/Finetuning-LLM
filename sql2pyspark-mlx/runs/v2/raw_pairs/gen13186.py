from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("launch_date") > "2024-03-07")

agg = (
    valid_products
    .groupBy(
        F.when(F.col("price") >= 1320, "high").when(F.col("price") >= 310, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("num_products"),
    )
)

result = (
    agg
    .filter(F.col("total_cost") < 2800)
    .select("price_bucket", "total_cost", "num_products")
    .orderBy(F.col("total_cost").desc())
)
