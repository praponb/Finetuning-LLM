from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") >= 2100)
    .select("category", "launch_cohort", "total_cost")
    .orderBy(F.col("total_cost").desc())
)
