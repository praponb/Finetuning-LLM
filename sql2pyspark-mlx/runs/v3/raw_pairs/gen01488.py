from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.stddev_salary") >= 47)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ebd.stddev_salary"),
        F.col("ebd.unique_depts"),
    )
)
