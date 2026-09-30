from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("budget_count") > 7)
)
