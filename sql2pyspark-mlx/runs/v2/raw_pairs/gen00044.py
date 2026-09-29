from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.lower(F.trim(F.col("e.email"))) == "grace.kim@gmail.com")
    .select(
        F.col("e.id"),
        F.col("e.hire_date"),
        F.col("e.level"),
        F.col("e.manager_id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
    .orderBy(F.col("budget").desc())
)
