from pyspark.sql import functions as F

filtered_products = df_products.filter((F.col("price") > 940) & F.array_contains("tags", "clearance"))

summary = (
    filtered_products
    .groupBy(
        "supplier_id",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    summary
    .filter(F.col("umbrella_count") < 23)
    .select("supplier_id", "launch_cohort", "stddev_price", "umbrella_count")
    .orderBy(F.col("supplier_id").desc())
)
