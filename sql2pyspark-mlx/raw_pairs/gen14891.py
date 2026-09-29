from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["HR", "Marketing"]))
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.countDistinct(F.col("e.salary")).alias("unique_depts"),
    )
    .orderBy(F.col("employee_count").desc(), F.col("id").desc())
)
