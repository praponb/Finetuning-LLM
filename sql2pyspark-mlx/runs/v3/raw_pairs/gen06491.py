from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("cost") < 480)

products_stats = (
    filtered_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    products_stats
    .select("launch_cohort", "stark_count", "cost_count")
    .orderBy("stark_count")
)
