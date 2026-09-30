from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Sports", "Home"]) & F.array_contains("tags", "premium"))
    .withColumn("brand_lower", F.lower("brand"))
)

summary = (
    src
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    summary
    .select("launch_month", "unique_brands")
    .orderBy(F.col("unique_brands").desc())
)
