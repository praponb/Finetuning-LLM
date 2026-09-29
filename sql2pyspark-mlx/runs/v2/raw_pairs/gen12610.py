from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.stddev_salary") > 55)
    .select(F.col("d.dept_name"), F.col("d.location"), F.col("ebd.stddev_salary"))
)
