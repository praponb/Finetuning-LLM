from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3755000, "high").when(F.col("budget") >= 1461000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .orderBy("dept_name", "unique_locations")
)
