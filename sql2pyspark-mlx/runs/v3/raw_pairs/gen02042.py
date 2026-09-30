from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.manager_salary") >= 43)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("dt.manager_salary"),
        F.col("dt.first_hire_date"),
    )
    .orderBy(F.col("manager_salary").desc())
)
