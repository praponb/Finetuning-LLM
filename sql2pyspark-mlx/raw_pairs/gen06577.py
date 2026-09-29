from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") > 8)
    .orderBy(F.col("launch_cohort").desc())
    .limit(3)
)
