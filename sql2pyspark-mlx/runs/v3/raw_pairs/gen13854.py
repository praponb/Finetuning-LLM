from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.director_salary") >= 41)
    .select(F.col("d.dept_name"), F.col("t.stddev_salary"), F.col("t.director_salary"))
    .orderBy("dept_name")
)
