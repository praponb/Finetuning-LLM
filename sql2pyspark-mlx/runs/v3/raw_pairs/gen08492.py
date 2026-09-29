from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count("*").alias("department_count"),
    )
    .orderBy(F.col("department_count").desc())
)
