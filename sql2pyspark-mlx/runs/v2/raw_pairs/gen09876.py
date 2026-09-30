from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 740, "high").when(F.col("cost") >= 110, "mid").otherwise("low").alias("cost_bucket"),
        "category",
    )
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("umbrella_count") > 2)
    .orderBy(F.col("category").desc(), "cost_bucket")
)
