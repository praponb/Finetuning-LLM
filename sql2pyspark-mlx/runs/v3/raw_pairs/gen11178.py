from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.designer_count") > 45)
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ds.designer_count"),
        F.col("ds.min_salary"),
    )
    .orderBy("location")
)
