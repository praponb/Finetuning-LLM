from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") <= "2024-01-01") & F.array_contains("tags", "eco"))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"))
    .filter(F.col("sports_count") < 1)
)
