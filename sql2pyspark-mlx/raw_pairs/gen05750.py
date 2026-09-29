from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 610, "high").when(F.col("cost") >= 130, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("price").alias("avg_price"),
        F.count("*").alias("product_count"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("cost")).otherwise(0)).alias("umbrella_cost"),
    )
    .filter(F.col("umbrella_cost") < 20)
    .drop("umbrella_cost")
    .orderBy(F.col("cost_bucket").desc())
)
