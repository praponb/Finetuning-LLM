from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.length("email") >= 7)
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("EmployeesByDepartment"), F.col("departments.id") == F.col("EmployeesByDepartment.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("EmployeesByDepartment.pct_l3"),
    )
    .orderBy(F.col("id").desc(), F.col("pct_l3").desc())
    .limit(5)
)
