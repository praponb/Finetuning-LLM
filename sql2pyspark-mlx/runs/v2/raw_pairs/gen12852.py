from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .orderBy(F.col("min_budget").desc())
)
