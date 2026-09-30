from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("level").isin(["L1", "L3"]))
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("E.salary_range"),
        F.col("E.min_hire_date"),
    )
    .orderBy("location")
)
