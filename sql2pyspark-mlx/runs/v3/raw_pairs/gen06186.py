from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("brand") == "Stark")

products_stats = (
    valid_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    products_stats
    .select("launch_cohort", "min_launch_date")
    .orderBy(F.col("min_launch_date").desc(), "launch_cohort")
)
