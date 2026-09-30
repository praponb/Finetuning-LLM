from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.when(F.col("cost") >= 700, "high").when(F.col("cost") >= 190, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("price").alias("price_count"))
    .filter(F.col("price_count") > 4)
    .select("brand", "cost_bucket", "price_count")
    .orderBy("brand")
)
