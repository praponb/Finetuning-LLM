from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 210)
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        "category",
        "brand",
    )
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .orderBy("brand", "launch_cohort", "category")
    .limit(10)
)
