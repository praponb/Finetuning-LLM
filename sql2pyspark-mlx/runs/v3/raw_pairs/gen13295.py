from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Designer", "Engineer"]))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.director_salary") >= 23)
    .select(F.col("d.location"), F.col("ebd.avg_salary"), F.col("ebd.director_salary"))
)
