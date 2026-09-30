from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") <= "2024-09-01"))
)

price_bucket_summary = (
    filtered_products
    .groupBy(
        F.when(F.col("price") >= 1040, "high").when(F.col("price") >= 440, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_categorys"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
)

result = (
    price_bucket_summary
    .select("price_bucket", "unique_categorys", "umbrella_count")
    .orderBy("umbrella_count")
)
