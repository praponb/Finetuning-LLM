from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.length("email") >= 7)
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.unique_depts") > 10)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ebd.total_salary"),
        F.col("ebd.unique_depts"),
    )
)
