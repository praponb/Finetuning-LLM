from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id").isin([6, 29, 33]))
    .groupBy("dept_id")
    .agg(F.count("*").alias("num_employees"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .filter(F.col("E.num_employees") > 7)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("E.num_employees"),
    )
    .orderBy("id", "num_employees")
)
