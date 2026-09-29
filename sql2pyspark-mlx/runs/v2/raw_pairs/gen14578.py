from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("salary") >= 87000)
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.unique_job_titles") > 7)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("dt.unique_job_titles"),
        F.col("dt.max_salary"),
    )
    .orderBy("unique_job_titles", "location", "id")
    .limit(1)
)
