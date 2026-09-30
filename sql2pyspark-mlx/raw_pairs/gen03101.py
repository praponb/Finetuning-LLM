from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

enriched = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.salary",
        "a.hire_date",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

result = (
    enriched
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "budget",
        "location",
        "dept_name",
        F.col("max_employee_salary"),
    )
    .orderBy("budget")
)
