from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget") > 1977000)
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.col("e.salary")).alias("total_salary"),
    )
    .orderBy(F.col("total_salary").desc())
)
