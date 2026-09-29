from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_job_titles") >= 4)
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ds.l5_count"),
        F.col("ds.unique_job_titles"),
    )
    .orderBy("l5_count")
)
